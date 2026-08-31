"""
fetch_sector_membership.py — Refresh the symbol->sector(s) membership map from NSE's
public constituent CSVs, cross-checked against the current Nifty200 universe.

Relocated 2026-08-31 from `backtesting/GapDC-Test/symbols/sector_membership.json`
(originally built there 2026-07-12, one-off, never refreshed since -- found stale: 48
symbols in that file no longer in the current 200-universe, and conversely at least one
current universe symbol, e.g. a recent IPO, missing from its natural sector because NSE's
own constituent list predates its listing). This project already owns SECTOR_INDICES /
instruments.py, so the membership map now lives alongside it rather than inside a single
backtest project -- a second consumer (the daily/5min sector bar stores work) is exactly
the kind of "a second project needs this" trigger the original file's own comment flagged.

NSE's constituent-CSV URLs aren't perfectly systematic (see CANDIDATE_SLUGS below for the
known gotchas -- financial services is `ind_niftyfinancelist.csv`, not
`ind_niftyfinancialserviceslist.csv`; there's no standalone niftyprivatebank file, private
banks fold into niftybank). Each sector tries its candidate slugs in order and uses the
first one that returns a real CSV with a `Symbol` column -- never guesses blind.

Usage:
    uv run python fetch_sector_membership.py              # refresh + write
    uv run python fetch_sector_membership.py --check-only  # just report drift, don't write

Output: sector_membership.json (repo root, sibling to instruments.py) --
    {"NIFTY IT": ["COFORGE", "HCLTECH", ...], ...}, one list per SECTOR_INDICES key.

NSE's own constituent lists only change on their periodic index-reconstitution reviews
(typically quarterly) -- a same-day refresh closes STALENESS (a list that's fallen behind
NSE's own last review) but not a brand-new IPO being sector-correct the day it lists.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
import truststore

truststore.inject_into_ssl()

from instruments import SECTOR_INDICES

OUT_PATH = Path(__file__).parent / "sector_membership.json"
UNIVERSE_PATH = Path(__file__).parent / "symbols" / "nifty200.txt"
BASE_URL = "https://nsearchives.nseindia.com/content/indices/ind_{slug}list.csv"
HEADERS = {"User-Agent": "Mozilla/5.0"}
SLEEP = 0.5

# Candidate URL slugs per sector, tried in order -- first one returning a real CSV wins.
# Confirmed live 2026-08-31 (all 13 resolved on their first candidate that day; kept as a
# list, not a single guess, since NSE's own naming isn't guaranteed stable).
CANDIDATE_SLUGS: dict[str, list[str]] = {
    "NIFTY IT": ["niftyit"],
    "NIFTY BANK": ["niftybank"],
    "NIFTY AUTO": ["niftyauto"],
    "NIFTY METAL": ["niftymetal"],
    "NIFTY FINANCIAL SERVICES": ["niftyfinance", "niftyfinancialserviceslist", "niftyfinancialservices"],
    "NIFTY PHARMA": ["niftypharma"],
    "NIFTY FMCG": ["niftyfmcg"],
    "NIFTY REALTY": ["niftyrealty"],
    "NIFTY MEDIA": ["niftymedia"],
    "NIFTY PSU BANK": ["niftypsubank"],
    "NIFTY HEALTHCARE": ["niftyhealthcare", "niftyhealthcareindex"],
    "NIFTY CONSUMER DURABLES": ["niftyconsumerdurables"],
    "NIFTY OIL AND GAS": ["niftyoilgas", "niftyoilandgas"],
}


def _fetch_one(sector: str) -> list[str] | None:
    for slug in CANDIDATE_SLUGS.get(sector, []):
        url = BASE_URL.format(slug=slug)
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
        except requests.RequestException as exc:
            print(f"  {sector}: {slug} -- request failed ({exc})")
            continue
        if r.status_code == 200 and b"Symbol" in r.content[:200]:
            df = pd.read_csv(StringIO(r.content.decode("utf-8")))
            return sorted(df["Symbol"].str.strip().str.upper().tolist())
        print(f"  {sector}: {slug} -- HTTP {r.status_code} or no Symbol column, trying next")
    return None


def build_membership() -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    failed = []
    for i, sector in enumerate(SECTOR_INDICES, 1):
        syms = _fetch_one(sector)
        if syms is None:
            print(f"[{i:2d}/{len(SECTOR_INDICES)}] {sector:26s} -- FAILED, no working URL")
            failed.append(sector)
        else:
            print(f"[{i:2d}/{len(SECTOR_INDICES)}] {sector:26s} -- {len(syms)} constituents")
            mapping[sector] = syms
        time.sleep(SLEEP)
    if failed:
        print(f"\n{len(failed)} sector(s) failed, not written for this run: {failed}")
    return mapping


def report_drift(mapping: dict[str, list[str]]) -> None:
    universe = {s.strip().upper() for s in UNIVERSE_PATH.read_text().splitlines() if s.strip()}
    all_mapped = {s for syms in mapping.values() for s in syms}

    covered = universe & all_mapped
    uncovered = sorted(universe - all_mapped)
    stale = sorted(all_mapped - universe)

    print(f"\nUniverse: {len(universe)} symbols")
    print(f"Covered by these {len(mapping)} sectors: {len(covered)}")
    print(f"Uncovered (no matching sector, or genuinely not in these 13): {len(uncovered)}")
    if uncovered:
        print(f"  {uncovered}")
    print(f"Stale (mapped but not in current universe -- delisted/replaced/renamed): {len(stale)}")
    if stale:
        print(f"  {stale}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Refresh sector_membership.json from NSE's constituent CSVs")
    parser.add_argument("--check-only", action="store_true", help="Report drift, don't write the file")
    args = parser.parse_args()

    mapping = build_membership()
    report_drift(mapping)

    if not args.check_only:
        OUT_PATH.write_text(json.dumps(mapping, indent=2))
        print(f"\nWrote {OUT_PATH}")
    else:
        print("\n--check-only: not written")
