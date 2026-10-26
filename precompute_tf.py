#!/usr/bin/env python3
"""
precompute_tf.py
=================
Roll all 5min Parquet files to an arbitrary N-minute TF and save to
data/{N}min/{SYMBOL}.parquet.

Generalized version of precompute_125min.py — same behavior, parametrized
by --minutes so it covers 15min, 25min, 75min (and 125min) without duplicating
the script per TF.

Usage
-----
  uv run python precompute_tf.py --minutes 15                  # build missing files only
  uv run python precompute_tf.py --minutes 15 --refresh        # rebuild stale files
  uv run python precompute_tf.py --minutes 15 --force          # rebuild all files
  uv run python precompute_tf.py --minutes 15 --symbol RELIANCE
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from rollup import rollup, available_symbols

BASE_DIR = Path(__file__).parent


def build(symbol: str, minutes: int, out_dir: Path, src_dir: Path, force: bool = False, refresh: bool = False) -> str:
    """
    Roll symbol's 5min data to `minutes` TF and write parquet.
    Returns status string: 'written', 'skipped', 'error:<msg>'.
    """
    out_path = out_dir / f"{symbol}.parquet"
    src_path = src_dir / f"{symbol}.parquet"

    if out_path.exists() and not force:
        if refresh:
            if src_path.stat().st_mtime <= out_path.stat().st_mtime:
                return "skipped"
        else:
            return "skipped"

    try:
        df = rollup(symbol, minutes=minutes)
        df = df.reset_index()
        df.to_parquet(out_path, index=False)
        return "written"
    except Exception as exc:
        return f"error:{exc}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Pre-compute N-min bars from 5min data")
    parser.add_argument("--minutes", type=int, required=True, help="Target TF in minutes (multiple of 5)")
    parser.add_argument("--symbol", help="Single symbol to process")
    parser.add_argument("--force", action="store_true", help="Rebuild all, even if up to date")
    parser.add_argument("--refresh", action="store_true", help="Rebuild only if 5min source is newer")
    args = parser.parse_args()

    if args.minutes % 5 != 0:
        print(f"--minutes must be a multiple of 5, got {args.minutes}")
        sys.exit(1)

    out_dir = BASE_DIR / "data" / f"{args.minutes}min"
    src_dir = BASE_DIR / "data" / "5min"
    out_dir.mkdir(parents=True, exist_ok=True)

    symbols = [args.symbol] if args.symbol else available_symbols()
    n = len(symbols)
    mode = "force" if args.force else ("refresh" if args.refresh else "missing-only")

    print(f"\nprecompute_tf --minutes {args.minutes}  --  {n} symbol(s)  [mode: {mode}]")
    print(f"Output : {out_dir}")
    print(f"Started: {datetime.now().strftime('%H:%M:%S')}")
    print("-" * 50)

    written = skipped = errors = 0

    for idx, sym in enumerate(symbols, 1):
        status = build(sym, args.minutes, out_dir, src_dir, force=args.force, refresh=args.refresh)
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
