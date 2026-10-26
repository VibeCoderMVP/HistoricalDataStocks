#!/usr/bin/env python3
"""
seed_tw_5min.py
===============
Prepend historical 5min bars from HistoricalDataStocks into TW's data/5min/*.csv files.

TW's P1 has been writing since 2026-06-01. This script fills in the gap by
prepending Apr+May 2026 bars (and anything else missing) so the 125min
indicator warmup (DC-20, VWMA-20 = 45 bars minimum) is fully warm from day 1.

The script never overwrites TW's existing rows — it only prepends dates that
don't already appear in the CSV.

Usage
-----
  uv run python seed_tw_5min.py                    # all 200 symbols
  uv run python seed_tw_5min.py --symbol RELIANCE  # one symbol
  uv run python seed_tw_5min.py --from 2026-01-01  # custom start date
"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

HDS_5MIN  = Path(__file__).parent / "data" / "5min"
TW_5MIN   = Path("D:/Trading/TradingWebSockets/data/5min")
SYMS_FILE = Path(__file__).parent / "symbols" / "nifty200.txt"

# Default: prepend from April 2026 onwards (well before TW's June 2026 start)
DEFAULT_FROM = "2026-04-01"


def load_symbols() -> list[str]:
    return [s.strip() for s in SYMS_FILE.read_text().splitlines() if s.strip()]


def seed_symbol(symbol: str, from_date: str) -> str:
    src = HDS_5MIN / f"{symbol}.parquet"
    dst = TW_5MIN  / f"{symbol}.csv"

    if not src.exists():
        return "no_src"

    # Load HDS parquet and convert to IST datetime strings
    raw = pd.read_parquet(src)
    raw["time"] = (
        pd.to_datetime(raw["timestamp"], unit="s", utc=True)
        .dt.tz_convert("Asia/Kolkata")
        .dt.tz_localize(None)   # strip tz → naive IST
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )
    raw = raw[["time", "open", "high", "low", "close", "volume"]].copy()

    # Filter to our target date range
    raw = raw[raw["time"] >= from_date]
    if raw.empty:
        return "no_data"

    # Load existing TW CSV (if it exists) to know which dates are already covered
    if dst.exists():
        existing = pd.read_csv(dst)
        existing.columns = [c.strip().lower() for c in existing.columns]
        existing_times = set(existing["time"].astype(str))
        # Only keep rows NOT already in TW CSV
        new_rows = raw[~raw["time"].isin(existing_times)]
        if new_rows.empty:
            return "already_seeded"
        # Combine: new historical rows first, then existing (preserves sort order)
        combined = pd.concat([new_rows, existing], ignore_index=True)
        combined = combined.sort_values("time").drop_duplicates(subset=["time"])
    else:
        combined = raw.sort_values("time").drop_duplicates(subset=["time"])

    # Atomic write (tmp → rename)
    tmp = dst.with_suffix(".tmp")
    combined.to_csv(tmp, index=False)
    tmp.replace(dst)

    added = len(combined) - (len(existing) if dst.exists() else 0)
    return f"added={added}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", help="Single symbol (default: all 200)")
    parser.add_argument("--from", dest="from_date", default=DEFAULT_FROM,
                        help=f"Start date YYYY-MM-DD (default: {DEFAULT_FROM})")
    args = parser.parse_args()

    TW_5MIN.mkdir(parents=True, exist_ok=True)
    symbols = [args.symbol] if args.symbol else load_symbols()
    n = len(symbols)

    print(f"\nseed_tw_5min  --  {n} symbol(s)  from={args.from_date}")
    print(f"  HDS source : {HDS_5MIN}")
    print(f"  TW target  : {TW_5MIN}")
    print("-" * 56)

    added = skipped = errors = 0
    for idx, sym in enumerate(symbols, 1):
        status = seed_symbol(sym, args.from_date)
        tag = ("SEED" if status.startswith("added") else
               "SKIP" if status in ("already_seeded", "no_data") else
               "ERR")
        print(f"  [{idx:3d}/{n}] {sym:<14} {tag}  {status}")
        if tag == "SEED":
            added += 1
        elif tag == "SKIP":
            skipped += 1
        else:
            errors += 1

    print("-" * 56)
    print(f"Done: {added} seeded  |  {skipped} skipped  |  {errors} errors")
    print(f"Finished: {datetime.now().strftime('%H:%M:%S')}")


if __name__ == "__main__":
    main()
