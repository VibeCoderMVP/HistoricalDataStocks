#!/usr/bin/env python3
"""
precompute_125min.py
====================
Roll all 5min Parquet files to 125min and save to data/125min/{SYMBOL}.parquet.

Run once to build the cache; re-run with --refresh to update only symbols
whose 5min data is newer than their existing 125min file.

Usage
-----
  uv run python precompute_125min.py                  # build missing files only
  uv run python precompute_125min.py --refresh        # rebuild stale files
  uv run python precompute_125min.py --force          # rebuild all files
  uv run python precompute_125min.py --symbol RELIANCE
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path


from rollup import rollup, available_symbols

OUT_DIR = Path(__file__).parent / "data" / "125min"
SRC_DIR = Path(__file__).parent / "data" / "5min"


def build(symbol: str, force: bool = False, refresh: bool = False) -> str:
    """
    Roll symbol's 5min data to 125min and write parquet.
    Returns status string: 'written', 'skipped', 'error:<msg>'.
    """
    out_path = OUT_DIR / f"{symbol}.parquet"
    src_path = SRC_DIR / f"{symbol}.parquet"

    if out_path.exists() and not force:
        if refresh:
            # Only rebuild if 5min source is newer than 125min cache
            if src_path.stat().st_mtime <= out_path.stat().st_mtime:
                return "skipped"
        else:
            return "skipped"

    try:
        df = rollup(symbol, minutes=125)
        df = df.reset_index()   # datetime becomes a column
        df.to_parquet(out_path, index=False)
        return "written"
    except Exception as exc:
        return f"error:{exc}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Pre-compute 125min bars from 5min data")
    parser.add_argument("--symbol",  help="Single symbol to process")
    parser.add_argument("--force",   action="store_true", help="Rebuild all, even if up to date")
    parser.add_argument("--refresh", action="store_true", help="Rebuild only if 5min source is newer")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    symbols = [args.symbol] if args.symbol else available_symbols()
    n = len(symbols)
    mode = "force" if args.force else ("refresh" if args.refresh else "missing-only")

    print(f"\nprecompute_125min  --  {n} symbol(s)  [mode: {mode}]")
    print(f"Output : {OUT_DIR}")
    print(f"Started: {datetime.now().strftime('%H:%M:%S')}")
    print("-" * 50)

    written = skipped = errors = 0

    for idx, sym in enumerate(symbols, 1):
        status = build(sym, force=args.force, refresh=args.refresh)
        tag = "WRITE" if status == "written" else ("SKIP" if status == "skipped" else "ERROR")
        print(f"[{idx:3d}/{n}] {sym:<14} {tag}" +
              (f"  {status[6:]}" if status.startswith("error") else ""))
        if status == "written":
            written += 1
        elif status == "skipped":
            skipped += 1
        else:
            errors += 1

    print("-" * 50)
    print(f"Done: {written} written  |  {skipped} skipped  |  {errors} errors")
    print(f"Finished: {datetime.now().strftime('%H:%M:%S')}")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
