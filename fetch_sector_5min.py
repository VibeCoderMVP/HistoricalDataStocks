"""
fetch_sector_5min.py — Download 5min OHLCV for NSE sector indices from Dhan.

Sibling to fetch_5min.py, same chunking/resume-safe approach, but for
exchange_segment=IDX_I / instrument_type=INDEX (Dhan's index feed) instead of
NSE_EQ/EQUITY. See instruments.py's SECTOR_INDICES dict and its docstring for
where these security_ids came from and the NIFTY FINANCIAL SERVICES proxy caveat.

Usage:
    uv run python fetch_sector_5min.py                    # all sector indices, 5yr
    uv run python fetch_sector_5min.py --sector "NIFTY IT" # single index
    uv run python fetch_sector_5min.py --years 1
    uv run python fetch_sector_5min.py --refresh           # incremental

Stored timestamps are Unix seconds UTC, same schema as data/5min/*.parquet
(symbol/timestamp/open/high/low/close/volume) except `symbol` holds the sector
index name (e.g. "NIFTY IT"), not an equity ticker.
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

from instruments import SECTOR_INDICES

DATA_DIR = Path(__file__).parent / "data" / "sector_5min"
ENV_PATH = Path("D:/Trading/TradingWebSockets/.env")
CHUNK_DAYS = 88
SLEEP = 0.25


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


def fetch_one(client: dhanhq, sector: str, security_id: str, years: int = 5,
              refresh: bool = False) -> pd.DataFrame | None:
    out = DATA_DIR / f"{sector}.parquet"
    today = datetime.now()

    if refresh and out.exists():
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
            exchange_segment="IDX_I",
            instrument_type="INDEX",
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

    if refresh and out.exists():
        existing = pd.read_parquet(out)
        merged = pd.concat([existing, new_df], ignore_index=True).drop_duplicates("timestamp").sort_values("timestamp")
        return merged.reset_index(drop=True)

    new_df["symbol"] = sector
    return new_df[["symbol", "timestamp", "open", "high", "low", "close", "volume"]].reset_index(drop=True)


def main(sectors: dict[str, str] | None = None, years: int = 5, refresh: bool = False) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    client = _client()
    universe = sectors or SECTOR_INDICES
    total = len(universe)
    failed = []

    for i, (sector, sec_id) in enumerate(universe.items(), 1):
        out = DATA_DIR / f"{sector}.parquet"
        if out.exists() and not refresh:
            print(f"[{i:2d}/{total}] {sector:28s} — exists, skip  (--refresh for incremental update)")
            continue

        df = fetch_one(client, sector, sec_id, years=years, refresh=refresh)
        if df is not None and not df.empty:
            df.to_parquet(out, index=False)
            start = datetime.utcfromtimestamp(int(df["timestamp"].min())).strftime("%Y-%m-%d")
            end   = datetime.utcfromtimestamp(int(df["timestamp"].max())).strftime("%Y-%m-%d")
            print(f"[{i:2d}/{total}] {sector:28s} - {len(df):6d} bars  ({start} to {end})")
        else:
            print(f"[{i:2d}/{total}] {sector:28s} — FAILED")
            failed.append(sector)

    print(f"\nDone. {total - len(failed)}/{total} sector indices saved.")
    if failed:
        print("Failed:", failed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch NSE sector index 5min OHLCV from Dhan")
    parser.add_argument("--sector", help='Single sector name (e.g. "NIFTY IT")')
    parser.add_argument("--years", type=int, default=5, help="Years of history (default 5)")
    parser.add_argument("--refresh", action="store_true", help="Incremental: only fetch new bars")
    args = parser.parse_args()

    syms = {args.sector: SECTOR_INDICES[args.sector]} if args.sector else None
    if args.sector and args.sector not in SECTOR_INDICES:
        print(f"ERROR: {args.sector!r} not in SECTOR_INDICES in instruments.py")
        sys.exit(1)
    main(sectors=syms, years=args.years, refresh=args.refresh)
