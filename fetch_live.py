#!/usr/bin/env python3
"""
F3L Live Data Fetcher — fetch_live.py
Fetches 2 weeks of Coinbase BTC-USD OHLCV candles (15m, 1h, 4h),
computes EMA/WMA indicators matching production bot, and pushes to GitHub.

Runs hourly via cron:
  0 * * * * cd /home/f3l_analysis/btc-live-data && nice -n 19 python3 fetch_live.py >> fetch.log 2>&1

Dependencies: Python 3 stdlib only. No pip installs.
"""

import urllib.request
import json
import csv
import os
import subprocess
import time
import sys
from datetime import datetime, timezone, timedelta

# ──────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
COINBASE_URL = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
LOOKBACK_DAYS = 14
MAX_CANDLES_PER_REQUEST = 300
REQUEST_TIMEOUT = 15  # seconds
RETRY_DELAY = 5       # seconds before retry

# Output files (written to SCRIPT_DIR, pushed to GitHub)
FILE_15M = os.path.join(SCRIPT_DIR, "live_15m.csv")
FILE_1H  = os.path.join(SCRIPT_DIR, "live_1h.csv")
FILE_4H  = os.path.join(SCRIPT_DIR, "live_4h.csv")


# ──────────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────────

def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)


# ──────────────────────────────────────────────────────────────────
# COINBASE API FETCHER
# ──────────────────────────────────────────────────────────────────

def fetch_candles(granularity_sec, lookback_days):
    """
    Fetch candles from Coinbase public API.
    
    CRITICAL: Coinbase returns [time, LOW, HIGH, OPEN, CLOSE, volume]
    This function reorders to standard OHLCV: (time, open, high, low, close, volume)
    
    Returns: dict keyed by unix timestamp -> (time, open, high, low, close, volume)
    """
    candles = {}  # dedup by timestamp
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=lookback_days)
    
    chunk_seconds = MAX_CANDLES_PER_REQUEST * granularity_sec
    chunk_start = start
    
    while chunk_start < now:
        chunk_end = min(chunk_start + timedelta(seconds=chunk_seconds), now)
        
        params = (
            f"?granularity={granularity_sec}"
            f"&start={chunk_start.strftime('%Y-%m-%dT%H:%M:%SZ')}"
            f"&end={chunk_end.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        )
        url = COINBASE_URL + params
        
        data = _api_request(url)
        if data is not None:
            for row in data:
                # Coinbase format: [time, LOW, HIGH, OPEN, CLOSE, volume]
                ts = int(row[0])
                low   = float(row[1])
                high  = float(row[2])
                opn   = float(row[3])
                close = float(row[4])
                vol   = float(row[5])
                # Reorder to OHLCV
                candles[ts] = (ts, opn, high, low, close, vol)
        
        chunk_start = chunk_end
    
    # Sort ascending by timestamp
    sorted_candles = [candles[ts] for ts in sorted(candles.keys())]
    return sorted_candles


def _api_request(url):
    """Make API request with one retry on failure."""
    for attempt in range(2):
        try:
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "F3L-DataBot/1.0")
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                data = json.loads(resp.read().decode())
                if not data:
                    log(f"  Empty response from {url[:80]}...")
                    return None
                return data
        except Exception as e:
            if attempt == 0:
                log(f"  Request failed (attempt 1): {e}. Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            else:
                log(f"  Request failed (attempt 2): {e}. Skipping chunk.")
                return None
    return None


# ──────────────────────────────────────────────────────────────────
# EMA / WMA COMPUTATION
# ──────────────────────────────────────────────────────────────────

def compute_ema_wma(candles_ohlcv, ema_period, wma_period):
    """
    Compute EMA and WMA (weighted moving average of EMA values).
    
    4H: EMA-20 (k=2/21) + WMA-14  — matches backtester recompute_ema_wma
    1H: EMA-24 (k=2/25) + WMA-50  — matches trend.py
    
    Args:
        candles_ohlcv: list of (time, open, high, low, close, volume)
        ema_period: EMA lookback (20 for 4H, 24 for 1H)
        wma_period: WMA lookback (14 for 4H, 50 for 1H)
    
    Returns: list of (time, open, high, low, close, ema, wma, volume)
    """
    if not candles_ohlcv:
        return []
    
    k = 2.0 / (ema_period + 1)
    
    # EMA seed: first candle's close price
    ema = candles_ohlcv[0][4]  # close of first candle
    ema_values = []
    
    result = []
    for i, (ts, opn, high, low, close, vol) in enumerate(candles_ohlcv):
        ema = close * k + ema * (1 - k)
        ema_values.append(ema)
        
        # WMA: 0.0 for first (wma_period - 1) candles
        if i < wma_period - 1:
            wma = 0.0
        else:
            # Weighted moving average of last wma_period EMA values
            # Weights: [1, 2, ..., wma_period]
            vals = ema_values[i - wma_period + 1 : i + 1]
            weights = list(range(1, wma_period + 1))
            wma = sum(v * w for v, w in zip(vals, weights)) / sum(weights)
        
        result.append((ts, opn, high, low, close, ema, wma, vol))
    
    return result


# ──────────────────────────────────────────────────────────────────
# 4H AGGREGATION FROM 1H
# ──────────────────────────────────────────────────────────────────

def aggregate_1h_to_4h(candles_1h_ohlcv):
    """
    Aggregate 1H candles into 4H candles.
    
    Aligns to UTC 4H boundaries: hours 0, 4, 8, 12, 16, 20.
    ONLY emits COMPLETE 4H groups (exactly 4 candles).
    Partial groups at the end are excluded to prevent false wicks.
    
    Args:
        candles_1h_ohlcv: list of (time, open, high, low, close, volume)
    
    Returns: list of (time, open, high, low, close, volume) for 4H candles
    """
    if not candles_1h_ohlcv:
        return []
    
    # Group by 4H boundary
    groups = {}  # boundary_ts -> list of 1H candles
    for candle in candles_1h_ohlcv:
        ts = candle[0]
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        h4_hour = (dt.hour // 4) * 4
        boundary_dt = dt.replace(hour=h4_hour, minute=0, second=0, microsecond=0)
        boundary_ts = int(boundary_dt.timestamp())
        
        if boundary_ts not in groups:
            groups[boundary_ts] = []
        groups[boundary_ts].append(candle)
    
    # Build 4H candles from COMPLETE groups only
    result = []
    for boundary_ts in sorted(groups.keys()):
        group = groups[boundary_ts]
        if len(group) != 4:
            continue  # Skip incomplete 4H groups
        
        # Sort by timestamp within group (should already be sorted)
        group.sort(key=lambda c: c[0])
        
        opn   = group[0][1]                          # first candle's open
        high  = max(c[2] for c in group)             # max of all highs
        low   = min(c[3] for c in group)             # min of all lows
        close = group[-1][4]                         # last candle's close
        vol   = sum(c[5] for c in group)             # sum of all volumes
        
        result.append((boundary_ts, opn, high, low, close, vol))
    
    return result


# ──────────────────────────────────────────────────────────────────
# CSV WRITERS
# ──────────────────────────────────────────────────────────────────

def write_csv_no_ema(filepath, candles_ohlcv):
    """
    Write 15m CSV: time,open,high,low,close,Volume
    No EMA/WMA columns.
    """
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['time', 'open', 'high', 'low', 'close', 'Volume'])
        for ts, opn, high, low, close, vol in candles_ohlcv:
            writer.writerow([int(ts), opn, high, low, close, vol])


def write_csv_with_ema(filepath, candles_with_ema):
    """
    Write 1H/4H CSV: time,open,high,low,close,EMA,EMA-based MA,Volume
    Matches existing TradingView CSV format.
    """
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['time', 'open', 'high', 'low', 'close', 'EMA', 'EMA-based MA', 'Volume'])
        for ts, opn, high, low, close, ema, wma, vol in candles_with_ema:
            writer.writerow([int(ts), opn, high, low, close, ema, wma, vol])


# ──────────────────────────────────────────────────────────────────
# GIT PUSH
# ──────────────────────────────────────────────────────────────────

def git_push():
    """Push updated CSVs to GitHub. Handles no-change and push-failure gracefully."""
    try:
        subprocess.run(
            ['git', 'add', 'live_15m.csv', 'live_1h.csv', 'live_4h.csv'],
            cwd=SCRIPT_DIR, check=True,
            capture_output=True, text=True
        )
    except subprocess.CalledProcessError as e:
        log(f"  git add failed: {e.stderr.strip()}")
        return False
    
    # Commit — returns non-zero if nothing to commit
    commit_msg = f"Update {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC"
    result = subprocess.run(
        ['git', 'commit', '-m', commit_msg],
        cwd=SCRIPT_DIR,
        capture_output=True, text=True
    )
    if result.returncode != 0:
        if 'nothing to commit' in result.stdout or 'nothing to commit' in result.stderr:
            log("  No data changes — skipping push.")
            return True
        log(f"  git commit failed: {result.stderr.strip()}")
        return False
    
    # Push
    try:
        subprocess.run(
            ['git', 'push'],
            cwd=SCRIPT_DIR, check=True,
            capture_output=True, text=True
        )
        log("  Git push successful.")
        return True
    except subprocess.CalledProcessError as e:
        log(f"  git push failed: {e.stderr.strip()}. Data saved locally.")
        return False


# ──────────────────────────────────────────────────────────────────
# VALIDATION
# ──────────────────────────────────────────────────────────────────

def validate_candles(candles, expected_interval, label):
    """Basic validation: check count, timestamp spacing, and data quality."""
    if not candles:
        log(f"  WARNING: {label} has 0 candles!")
        return False
    
    log(f"  {label}: {len(candles)} candles")
    
    # Check timestamp spacing
    bad_gaps = 0
    for i in range(1, min(len(candles), 20)):  # check first 20
        gap = candles[i][0] - candles[i-1][0]
        if gap != expected_interval:
            bad_gaps += 1
    
    if bad_gaps > 0:
        log(f"  WARNING: {label} has {bad_gaps} irregular gaps in first 20 candles")
    
    # Check time range
    first_dt = datetime.fromtimestamp(candles[0][0], tz=timezone.utc)
    last_dt = datetime.fromtimestamp(candles[-1][0], tz=timezone.utc)
    log(f"  {label}: {first_dt:%Y-%m-%d %H:%M} → {last_dt:%Y-%m-%d %H:%M} UTC")
    
    return True


# ──────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────

def main():
    log("=" * 60)
    log("F3L Live Data Fetch — Starting")
    log("=" * 60)
    
    # ── 1. Fetch 15m candles ──
    log("Fetching 15m candles...")
    candles_15m = fetch_candles(900, LOOKBACK_DAYS)
    if not validate_candles(candles_15m, 900, "15m"):
        log("FATAL: No 15m data fetched. Exiting.")
        sys.exit(1)
    
    # ── 2. Fetch 1h candles ──
    log("Fetching 1h candles...")
    candles_1h = fetch_candles(3600, LOOKBACK_DAYS)
    if not validate_candles(candles_1h, 3600, "1h"):
        log("FATAL: No 1h data fetched. Exiting.")
        sys.exit(1)
    
    # ── 3. Aggregate 1h → 4h ──
    log("Aggregating 1h → 4h...")
    candles_4h = aggregate_1h_to_4h(candles_1h)
    if not validate_candles(candles_4h, 14400, "4h"):
        log("WARNING: No complete 4h groups found.")
    
    # ── 4. Compute EMA/WMA ──
    log("Computing 1h EMA-24 + WMA-50...")
    candles_1h_ema = compute_ema_wma(candles_1h, ema_period=24, wma_period=50)
    
    log("Computing 4h EMA-20 + WMA-14...")
    candles_4h_ema = compute_ema_wma(candles_4h, ema_period=20, wma_period=14)
    
    # ── 5. Write CSVs ──
    log("Writing CSVs...")
    write_csv_no_ema(FILE_15M, candles_15m)
    log(f"  Wrote {FILE_15M} ({len(candles_15m)} rows)")
    
    write_csv_with_ema(FILE_1H, candles_1h_ema)
    log(f"  Wrote {FILE_1H} ({len(candles_1h_ema)} rows)")
    
    write_csv_with_ema(FILE_4H, candles_4h_ema)
    log(f"  Wrote {FILE_4H} ({len(candles_4h_ema)} rows)")
    
    # ── 6. Spot-check EMA/WMA values ──
    if candles_4h_ema:
        last = candles_4h_ema[-1]
        log(f"  4h last candle: time={last[0]} close={last[4]:.2f} "
            f"EMA={last[5]:.2f} WMA={last[6]:.2f}")
    if candles_1h_ema:
        last = candles_1h_ema[-1]
        log(f"  1h last candle: time={last[0]} close={last[4]:.2f} "
            f"EMA={last[5]:.2f} WMA={last[6]:.2f}")
    
    # ── 7. Git push ──
    log("Pushing to GitHub...")
    git_push()
    
    log("Done.")
    log("=" * 60)


if __name__ == "__main__":
    main()
