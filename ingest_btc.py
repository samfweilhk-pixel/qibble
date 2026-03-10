"""
Binance BTC/USDT 1-min kline ingestion → Parquet
=================================================
- 5 years of history (UTC days)
- Conservative rate limiting: 1 req / 600ms (~100/min, 6x under Binance limit)
- Resume-safe: checks existing parquet and picks up from last timestamp
- Incremental saves every 500 requests (never lose progress)
- Pre-computes: buy/sell vol, net_flow, bar_imbalance, pct_return, avg_trade_size
"""

import os
import sys
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta

# Force unbuffered stdout
sys.stdout.reconfigure(line_buffering=True)

# ── Config ──────────────────────────────────────────────────────────────
SYMBOL = "BTCUSDT"
INTERVAL = "1m"
BARS_PER_REQUEST = 1000
REQUEST_DELAY_S = 0.6  # 600ms between requests → ~100 req/min
SAVE_EVERY = 500  # Write parquet every N requests
OUTDIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
OUTFILE = os.path.join(OUTDIR, "btc_1m.parquet")

# 5 years back from today
END_DT = datetime.now(timezone.utc)
START_DT = END_DT - timedelta(days=5 * 365)

BINANCE_URL = "https://data-api.binance.vision/api/v3/klines"

COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "num_trades",
    "taker_buy_vol", "taker_buy_quote_vol", "ignore",
]


def fetch_klines(start_ms: int, end_ms: int) -> list:
    """Fetch up to 1000 klines from Binance. Returns raw list of lists."""
    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": BARS_PER_REQUEST,
    }
    resp = requests.get(BINANCE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def to_dataframe(raw_bars: list) -> pd.DataFrame:
    """Convert raw Binance kline arrays to typed DataFrame."""
    df = pd.DataFrame(raw_bars, columns=COLUMNS)

    # Timestamps
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

    # Numeric columns
    float_cols = ["open", "high", "low", "close", "volume",
                  "quote_volume", "taker_buy_vol", "taker_buy_quote_vol"]
    for c in float_cols:
        df[c] = df[c].astype(float)
    df["num_trades"] = df["num_trades"].astype(int)

    # Drop useless column
    df.drop(columns=["ignore"], inplace=True)

    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add pre-computed derived metrics."""
    df["buy_vol"] = df["taker_buy_vol"]
    df["sell_vol"] = df["volume"] - df["taker_buy_vol"]
    df["net_flow"] = df["buy_vol"] - df["sell_vol"]
    df["bar_imbalance"] = np.where(
        df["volume"] > 0,
        df["net_flow"] / df["volume"],
        0.0,
    )
    df["pct_return"] = (df["close"] / df["open"] - 1) * 100
    df["avg_trade_size"] = np.where(
        df["num_trades"] > 0,
        df["volume"] / df["num_trades"],
        0.0,
    )

    # UTC date for grouping
    df["date_utc"] = df["open_time"].dt.date.astype(str)

    return df


def save_parquet(new_chunks: list):
    """Save chunks to parquet, merging with existing file if present."""
    new_df = pd.concat(new_chunks, ignore_index=True)

    if os.path.exists(OUTFILE):
        existing = pd.read_parquet(OUTFILE)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined.drop_duplicates(subset=["open_time"], keep="last", inplace=True)
        combined.sort_values("open_time", inplace=True)
        combined.reset_index(drop=True, inplace=True)
        final_df = combined
    else:
        new_df.sort_values("open_time", inplace=True)
        new_df.reset_index(drop=True, inplace=True)
        final_df = new_df

    final_df.to_parquet(OUTFILE, index=False, engine="pyarrow")
    file_mb = os.path.getsize(OUTFILE) / 1_048_576
    print(f"  💾 SAVED: {len(final_df):,} total bars, {file_mb:.1f} MB")
    return final_df


def get_resume_start_ms() -> int:
    """If parquet exists, resume from last bar + 1 minute."""
    if os.path.exists(OUTFILE):
        existing = pd.read_parquet(OUTFILE, columns=["open_time"])
        if len(existing) > 0:
            last_ts = existing["open_time"].max()
            resume_ms = int(last_ts.timestamp() * 1000) + 60_000
            print(f"  Resuming from {last_ts} ({len(existing):,} bars already cached)")
            return resume_ms
    return int(START_DT.timestamp() * 1000)


def main():
    start_ms = get_resume_start_ms()
    end_ms = int(END_DT.timestamp() * 1000)

    if start_ms >= end_ms:
        print("Already up to date.")
        return

    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
    total_minutes = (end_ms - start_ms) / 60_000
    est_requests = int(total_minutes / BARS_PER_REQUEST) + 1

    print(f"BTC/USDT 1-min ingestion")
    print(f"  Range: {start_dt:%Y-%m-%d %H:%M} → {end_dt:%Y-%m-%d %H:%M} UTC")
    print(f"  Est. bars: {total_minutes:,.0f} | Est. requests: {est_requests:,}")
    print(f"  Rate: 1 req / {REQUEST_DELAY_S}s → ~{60/REQUEST_DELAY_S:.0f} req/min")
    print(f"  Est. time: {est_requests * REQUEST_DELAY_S / 60:.1f} min")
    print()

    all_chunks = []
    cursor_ms = start_ms
    req_count = 0
    total_bars = 0
    t_start = time.time()

    while cursor_ms < end_ms:
        # Fetch with timing
        t0 = time.time()
        raw = fetch_klines(cursor_ms, end_ms)
        latency_ms = (time.time() - t0) * 1000
        req_count += 1

        if not raw:
            print(f"  Empty response at req {req_count}, cursor={cursor_ms}. Stopping.")
            break

        chunk_df = to_dataframe(raw)
        chunk_df = add_derived_columns(chunk_df)
        all_chunks.append(chunk_df)
        total_bars += len(chunk_df)

        # Progress — every 10 requests
        last_time = chunk_df["open_time"].iloc[-1]
        pct = min(100, (cursor_ms - start_ms) / (end_ms - start_ms) * 100)
        elapsed = time.time() - t_start
        rate = req_count / elapsed * 60 if elapsed > 0 else 0
        eta_min = (est_requests - req_count) / rate if rate > 0 else 0

        if req_count % 10 == 0 or req_count <= 3 or len(raw) < BARS_PER_REQUEST:
            print(f"  [{pct:5.1f}%] req={req_count:,} bars={total_bars:,} "
                  f"latency={latency_ms:.0f}ms rate={rate:.0f}/min "
                  f"ETA={eta_min:.1f}min last={last_time}")

        # Incremental save every SAVE_EVERY requests
        if req_count % SAVE_EVERY == 0:
            save_parquet(all_chunks)
            all_chunks = []  # Free memory after save

        # Advance cursor past the last bar we received
        cursor_ms = int(chunk_df["open_time"].iloc[-1].timestamp() * 1000) + 60_000

        # If we got fewer than requested, we've hit the end
        if len(raw) < BARS_PER_REQUEST:
            break

        # Rate limit — be polite
        time.sleep(REQUEST_DELAY_S)

    # Final save
    if all_chunks:
        final_df = save_parquet(all_chunks)
    elif os.path.exists(OUTFILE):
        final_df = pd.read_parquet(OUTFILE)
    else:
        print("No data fetched.")
        return

    elapsed_total = time.time() - t_start
    file_mb = os.path.getsize(OUTFILE) / 1_048_576

    print()
    print(f"Done!")
    print(f"  Total bars: {len(final_df):,}")
    print(f"  Date range: {final_df['open_time'].min()} → {final_df['open_time'].max()}")
    print(f"  File: {OUTFILE}")
    print(f"  Size: {file_mb:.1f} MB")
    print(f"  Requests: {req_count:,} in {elapsed_total/60:.1f} min")

    # Quick sanity check
    print()
    print("Sanity check (last 5 bars):")
    cols = ["open_time", "close", "volume", "buy_vol", "sell_vol", "bar_imbalance", "num_trades"]
    print(final_df[cols].tail().to_string(index=False))


if __name__ == "__main__":
    main()
