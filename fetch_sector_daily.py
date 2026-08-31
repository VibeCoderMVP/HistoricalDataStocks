"""
fetch_sector_daily.py — Download daily OHLC for NSE sector indices from Dhan.

Sibling to fetch_index_daily.py, same call shape (historical_daily_data, single
call per index -- no chunking needed, unlike the 5min intraday endpoint), but
reads instruments.py's SECTOR_INDICES dict instead of BROAD_INDICES.

Unlike broad-market indices (fetch_index_daily.py's NIFTY 50), these 13 sector indices carry
REAL non-zero volume from Dhan -- confirmed live 2026-08-31 (e.g. NIFTY IT ~20-44M/day).
Likely a real aggregate summed across constituent stocks, same finding already confirmed for
the 5min sector store (see sector_bars.py's own docstring / project_sector_bars_live memory).

Usage:
    uv run python fetch_sector_daily.py                    # all 13 sector indices
    uv run python fetch_sector_daily.py --sector "NIFTY IT"
    uv run python fetch_sector_daily.py --force             # re-fetch even if file exists

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

from instruments import SECTOR_INDICES

DATA_DIR = Path(__file__).parent / "data" / "sector_daily"
ENV_PATH = Path("D:/Trading/TradingWebSockets/.env")
SLEEP = 0.25


def _client() -> dhanhq:
    load_dotenv(dotenv_path=ENV_PATH)
    ctx = DhanContext(os.environ["DHAN_CLIENT_ID"], os.environ["DHAN_ACCESS_TOKEN"])
    return dhanhq(ctx)


def fetch_one(client: dhanhq, sector: str, security_id: str, years: int = 5) -> pd.DataFrame | None:
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
    if not data.get("timestamp"):
        return None
    df = pd.DataFrame({
        "date":   pd.to_datetime(data["timestamp"], unit="s").normalize(),
        "open":   data["open"],
        "high":   data["high"],
        "low":    data["low"],
        "close":  data["close"],
        "volume": data["volume"],
    })
    df["symbol"] = sector
    return df[["symbol", "date", "open", "high", "low", "close", "volume"]].sort_values("date").reset_index(drop=True)


def main(sectors: dict[str, str] | None = None, years: int = 5, force: bool = False) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    client = _client()
    universe = sectors or SECTOR_INDICES
    total = len(universe)
    failed = []

    for i, (name, sec_id) in enumerate(universe.items(), 1):
        out = DATA_DIR / f"{name}.parquet"
        if out.exists() and not force:
            print(f"[{i:3d}/{total}] {name:26s} — exists, skip  (--force to refetch)")
            continue
        df = fetch_one(client, name, sec_id, years)
        if df is not None and not df.empty:
            df.to_parquet(out, index=False)
            print(f"[{i:3d}/{total}] {name:26s} - {len(df):4d} bars  ({df['date'].min().date()} to {df['date'].max().date()})")
        else:
            print(f"[{i:3d}/{total}] {name:26s} — FAILED")
            failed.append(name)
        time.sleep(SLEEP)

    print(f"\nDone. {total - len(failed)}/{total} sector indices saved.")
    if failed:
        print("Failed:", failed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch NSE sector index daily OHLC from Dhan")
    parser.add_argument("--sector", help='Single sector name (e.g. "NIFTY IT")')
    parser.add_argument("--years", type=int, default=5, help="Years of history (default 5)")
    parser.add_argument("--force", action="store_true", help="Re-fetch even if file exists")
    args = parser.parse_args()

    sel = {args.sector: SECTOR_INDICES[args.sector]} if args.sector else None
    if args.sector and args.sector not in SECTOR_INDICES:
        print(f"ERROR: {args.sector!r} not in SECTOR_INDICES map in instruments.py")
        sys.exit(1)
    main(sectors=sel, years=args.years, force=args.force)
