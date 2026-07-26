"""
fetch_index_daily.py — Download 5 years of daily OHLC for broad-market indices
(NIFTY 50 etc.) from Dhan.

Sibling to fetch_daily.py, same call shape, but exchange_segment=IDX_I /
instrument_type=INDEX (Dhan's index feed) instead of NSE_EQ/EQUITY — same
pattern already used by fetch_sector_5min.py for sector indices. See
instruments.py's BROAD_INDICES dict.

Indices have no volume; Dhan returns 0 for that field, stored as-is.

Usage:
    uv run python fetch_index_daily.py                  # all broad indices
    uv run python fetch_index_daily.py --index "NIFTY 50"
    uv run python fetch_index_daily.py --force           # re-fetch even if file exists

Same Dhan date-shift caveat as fetch_daily.py applies (see that file's docstring).
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

from instruments import BROAD_INDICES

DATA_DIR = Path(__file__).parent / "data" / "index_daily"
ENV_PATH = Path("D:/Trading/TradingWebSockets/.env")
SLEEP = 0.25


def _client() -> dhanhq:
    load_dotenv(dotenv_path=ENV_PATH)
    ctx = DhanContext(os.environ["DHAN_CLIENT_ID"], os.environ["DHAN_ACCESS_TOKEN"])
    return dhanhq(ctx)


def fetch_one(client: dhanhq, name: str, security_id: str, years: int = 5) -> pd.DataFrame | None:
    today = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=years * 365)).strftime("%Y-%m-%d")
    r = client.historical_daily_data(
        security_id=security_id,
        exchange_segment="IDX_I",
        instrument_type="INDEX",
        from_date=from_date,
        to_date=today,
        expiry_code=0,
    )
    if r.get("status") != "success" or not isinstance(r.get("data"), dict):
        return None
    data = r["data"]
    df = pd.DataFrame({
        "date":   pd.to_datetime(data["timestamp"], unit="s").normalize(),
        "open":   data["open"],
        "high":   data["high"],
        "low":    data["low"],
        "close":  data["close"],
        "volume": data["volume"],
    })
    df["symbol"] = name
    return df[["symbol", "date", "open", "high", "low", "close", "volume"]].sort_values("date").reset_index(drop=True)


def main(indices: dict[str, str] | None = None, years: int = 5, force: bool = False) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    client = _client()
    universe = indices or BROAD_INDICES
    total = len(universe)
    failed = []

    for i, (name, sec_id) in enumerate(universe.items(), 1):
        fname = name.replace(" ", "")
        out = DATA_DIR / f"{fname}.parquet"
        if out.exists() and not force:
            print(f"[{i:3d}/{total}] {name:16s} — exists, skip  (--force to refetch)")
            continue
        df = fetch_one(client, name, sec_id, years)
        if df is not None and not df.empty:
            df.to_parquet(out, index=False)
            print(f"[{i:3d}/{total}] {name:16s} - {len(df):4d} bars  ({df['date'].min().date()} to {df['date'].max().date()})")
        else:
            print(f"[{i:3d}/{total}] {name:16s} — FAILED")
            failed.append(name)
        time.sleep(SLEEP)

    print(f"\nDone. {total - len(failed)}/{total} indices saved.")
    if failed:
        print("Failed:", failed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch broad-market index daily OHLC from Dhan")
    parser.add_argument("--index", help='Single index name (e.g. "NIFTY 50")')
    parser.add_argument("--years", type=int, default=5, help="Years of history (default 5)")
    parser.add_argument("--force", action="store_true", help="Re-fetch even if file exists")
    args = parser.parse_args()

    idx = {args.index: BROAD_INDICES[args.index]} if args.index else None
    if args.index and args.index not in BROAD_INDICES:
        print(f"ERROR: {args.index!r} not in BROAD_INDICES map in instruments.py")
        sys.exit(1)
    main(indices=idx, years=args.years, force=args.force)
