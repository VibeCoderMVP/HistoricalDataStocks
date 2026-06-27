"""
fetch_5min.py — Download 5min OHLCV for Nifty 200 from Dhan.

Dhan allows max 90 days per call for intraday data.
Chunks 5 years into 88-day windows and merges into a single Parquet per symbol.
Resume-safe: if a Parquet already exists, only fetches from its last timestamp onward.

Usage:
    uv run python fetch_5min.py                   # all Nifty 200, 5yr (initial fetch ~18 min)
    uv run python fetch_5min.py --symbol RELIANCE # single symbol
    uv run python fetch_5min.py --years 1         # 1yr only
    uv run python fetch_5min.py --refresh         # fetch only missing/new bars (incremental)

Rate: 4 calls/sec → 21 chunks * 200 symbols = 4,200 calls = ~18 minutes for full Nifty 200.

Stored timestamps are Unix seconds UTC. Use rollup.py for IST-aware aggregation.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import truststore

truststore.inject_into_ssl()

from dhanhq import DhanContext, dhanhq
from dotenv import load_dotenv

from instruments import NIFTY_200

DATA_DIR = Path(__file__).parent / "data" / "5min"
ENV_PATH = Path("D:/Trading/TradingWebSockets/.env")
CHUNK_DAYS = 88   # safely under 90-day API limit
SLEEP = 0.25      # 4 req/sec


def _client() -> dhanhq:
    load_dotenv(dotenv_path=ENV_PATH)
    ctx = DhanContext(os.environ["DHAN_CLIENT_ID"], os.environ["DHAN_ACCESS_TOKEN"])
    return dhanhq(ctx)


def _make_chunks(from_dt: datetime, to_dt: datetime) -> list[tuple[str, str]]:
    chunks = []
    cur = from_dt
    while cur < to_dt:
        end = min(cur + timedelta(days=CHUNK_DAYS), to_dt)
        chunks.append((cur.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
        cur = end
    return chunks


def _last_stored_ts(path: Path) -> datetime | None:
    if not path.exists():
        return None
    df = pd.read_parquet(path, columns=["timestamp"])
    if df.empty:
        return None
    return datetime.utcfromtimestamp(int(df["timestamp"].max()))


def fetch_one(
    client: dhanhq,
    symbol: str,
    security_id: str,
    years: int = 5,
    refresh: bool = False,
) -> pd.DataFrame | None:
    out = DATA_DIR / f"{symbol}.parquet"
    today = datetime.now()

    if refresh and out.exists():
        # Only fetch new bars since last stored timestamp
        last_ts = _last_stored_ts(out)
        from_dt = last_ts + timedelta(seconds=1) if last_ts else today - timedelta(days=years * 365)
    else:
        from_dt = today - timedelta(days=years * 365)

    chunks = _make_chunks(from_dt, today)
    if not chunks:
        return None

    frames = []
    for from_str, to_str in chunks:
        r = client.intraday_minute_data(
            security_id=security_id,
            exchange_segment="NSE_EQ",
            instrument_type="EQUITY",
            from_date=from_str,
            to_date=to_str,
            interval=5,
        )
        if r.get("status") == "success" and isinstance(r.get("data"), dict):
            data = r["data"]
            if data.get("timestamp"):
                frames.append(pd.DataFrame({
                    "timestamp": data["timestamp"],
                    "open":      data["open"],
                    "high":      data["high"],
                    "low":       data["low"],
                    "close":     data["close"],
                    "volume":    data["volume"],
                }))
        time.sleep(SLEEP)

    if not frames:
        return None

    new_df = pd.concat(frames, ignore_index=True).drop_duplicates("timestamp").sort_values("timestamp")

    # Merge with existing if refresh
    if refresh and out.exists():
        existing = pd.read_parquet(out)
        merged = pd.concat([existing, new_df], ignore_index=True).drop_duplicates("timestamp").sort_values("timestamp")
        return merged.reset_index(drop=True)

    new_df["symbol"] = symbol
    return new_df[["symbol", "timestamp", "open", "high", "low", "close", "volume"]].reset_index(drop=True)


def main(symbols: dict[str, str] | None = None, years: int = 5, refresh: bool = False) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    client = _client()
    universe = symbols or NIFTY_200
    total = len(universe)
    failed = []

    for i, (symbol, sec_id) in enumerate(universe.items(), 1):
        out = DATA_DIR / f"{symbol}.parquet"
        if out.exists() and not refresh:
            print(f"[{i:3d}/{total}] {symbol:16s} — exists, skip  (--refresh for incremental update)")
            continue

        df = fetch_one(client, symbol, sec_id, years=years, refresh=refresh)
        if df is not None and not df.empty:
            df.to_parquet(out, index=False)
            start = datetime.utcfromtimestamp(int(df["timestamp"].min())).strftime("%Y-%m-%d")
            end   = datetime.utcfromtimestamp(int(df["timestamp"].max())).strftime("%Y-%m-%d")
            print(f"[{i:3d}/{total}] {symbol:16s} - {len(df):6d} bars  ({start} to {end})")
        else:
            print(f"[{i:3d}/{total}] {symbol:16s} — FAILED")
            failed.append(symbol)

    print(f"\nDone. {total - len(failed)}/{total} symbols saved.")
    if failed:
        print("Failed:", failed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Nifty 200 5min OHLCV from Dhan")
    parser.add_argument("--symbol", help="Single symbol (e.g. RELIANCE)")
    parser.add_argument("--years", type=int, default=5, help="Years of history (default 5)")
    parser.add_argument("--refresh", action="store_true", help="Incremental: only fetch new bars")
    args = parser.parse_args()

    syms = {args.symbol: NIFTY_200[args.symbol]} if args.symbol else None
    if args.symbol and args.symbol not in NIFTY_200:
        print(f"ERROR: {args.symbol} not in NIFTY_200 map in instruments.py")
        sys.exit(1)
    main(symbols=syms, years=args.years, refresh=args.refresh)
