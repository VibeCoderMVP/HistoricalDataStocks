"""
fetch_corporate_actions.py — Download NSE's authoritative corporate-actions
archive and derive confirmed split/bonus/consolidation events with exact
price-adjustment ratios, for use as ground truth against the daily OHLCV
store (which carries NO split adjustment — see CLAUDE.md "Known Limitations").

This replaces price-ratio guessing (detect_split_candidates.py's heuristic)
with NSE's own ex-date + subject text. Confirmed live 2026-07-26: NSE's
corporates-corporateActions API returns the full requested date range in one
call (no 90-day chunking needed, unlike Dhan intraday), but requires a
browser-shaped session: hit a referer page first to get a valid cookie jar,
then call the API with the same session. Direct GET to the bare API without
that warm-up 403s.

Endpoint + session pattern taken from the existing
BhavCopy/temp-utility/NseUtility/NseUtility.py get_corporate_action() method
(already used elsewhere in this repo for NSE data) — same technique, adapted
to also parse ratios and combine same-day multi-action events.

Ratio convention (price[after] / price[before]):
  "Bonus N:M"                              -> ratio = M / (N + M)
    (N new shares issued per M held; e.g. "Bonus 1:4" -> 4/5 = 0.80)
  "Face Value Split ... From Rs X To Rs Y" -> ratio = Y / X
    (e.g. Rs 10 -> Rs 2 is a 5:1 split -> 2/10 = 0.20)
  "Consolidation ... From Rs X To Rs Y"    -> ratio = Y / X (reverse split, ratio > 1)

When a symbol has multiple actions on the same ex-date (e.g. BAJAJFINSV
2022-09-13: face-value split Rs5->Re1 AND Bonus 1:1 same day), the combined
ratio is the PRODUCT of the individual ratios — that's the actual expected
overnight price-scale change.

Output:
  data/corporate_actions_raw.parquet     — full unfiltered NSE feed (all symbols, all action types) for reference
  corporate_actions_splits.csv           — bonus/split/consolidation rows only, NIFTY_200 universe, with parsed ratio
  confirmed_splits.json                  — {symbol: [{date, combined_ratio, actions: [subject, ...]}]}, ready to
                                            cross-check against detect_split_candidates.py's heuristic output or
                                            feed directly into a backtest as an adjustment/exclude list

Usage:
    uv run python fetch_corporate_actions.py
    uv run python fetch_corporate_actions.py --from-date 01-01-2021 --to-date 31-12-2026
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd
import truststore

truststore.inject_into_ssl()

import requests

from instruments import NIFTY_200

DATA_DIR = Path(__file__).parent / "data"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}
REF_URL = "https://www.nseindia.com/companies-listing/corporate-filings-actions"
API_URL = "https://www.nseindia.com/api/corporates-corporateActions?index=equities&from_date={f}&to_date={t}"

SPLIT_SUBJECT_RE = re.compile(r"bonus|split|sub.?divi|consolidat", re.I)
BONUS_RATIO_RE = re.compile(r"bonus\s+(\d+)\s*:\s*(\d+)", re.I)
FACE_VALUE_RE = re.compile(
    r"from\s+r[es]\.?\s*([\d.]+)\s*/?-?\s*per\s+share\s+to\s+r[es]\.?\s*([\d.]+)", re.I
)


def _fetch_raw(from_date: str, to_date: str) -> pd.DataFrame:
    session = requests.Session()
    session.get(REF_URL, headers=HEADERS, timeout=15)
    resp = session.get(API_URL.format(f=from_date, t=to_date), headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return pd.DataFrame(resp.json())


def _parse_ratio(subject: str) -> tuple[float | None, str]:
    """Return (ratio, kind) for a single action's subject text, or (None, 'unparsed')."""
    subject = subject.strip()
    m = BONUS_RATIO_RE.search(subject)
    if m:
        n, mm = int(m.group(1)), int(m.group(2))
        return mm / (n + mm), "bonus"
    m = FACE_VALUE_RE.search(subject)
    if m:
        old_fv, new_fv = float(m.group(1)), float(m.group(2))
        if old_fv > 0:
            kind = "split" if new_fv < old_fv else "consolidation"
            return new_fv / old_fv, kind
    return None, "unparsed"


def main(from_date: str, to_date: str) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Fetching NSE corporate actions {from_date} -> {to_date} ...")
    raw = _fetch_raw(from_date, to_date)
    print(f"Fetched {len(raw)} total corporate-action records (all symbols, all types).")

    raw_out = DATA_DIR / "corporate_actions_raw.parquet"
    raw.to_parquet(raw_out, index=False)
    print(f"Wrote {raw_out}")

    universe = set(NIFTY_200.keys())
    splits = raw[raw["symbol"].isin(universe) & raw["subject"].str.contains(SPLIT_SUBJECT_RE, na=False)].copy()
    splits["ratio"], splits["kind"] = zip(*splits["subject"].map(_parse_ratio))
    splits["exDate_parsed"] = pd.to_datetime(splits["exDate"], format="%d-%b-%Y", errors="coerce")
    splits = splits.sort_values(["symbol", "exDate_parsed"])

    unparsed = splits[splits["ratio"].isna()]
    if len(unparsed):
        print(f"\nWARNING: {len(unparsed)} split/bonus-like rows didn't match a known subject pattern — "
              f"needs a manual look, ratio left blank:")
        print(unparsed[["symbol", "exDate", "subject"]].to_string(index=False))

    out_csv = Path(__file__).parent / "corporate_actions_splits.csv"
    splits.drop(columns=["exDate_parsed"]).to_csv(out_csv, index=False)
    print(f"\nWrote {out_csv} — {len(splits)} split/bonus/consolidation rows across "
          f"{splits['symbol'].nunique()} NIFTY_200 symbols.")

    # Combine same-day multi-action events (e.g. split + bonus same ex-date) into one entry
    confirmed: dict[str, list[dict]] = {}
    for (symbol, date), grp in splits.groupby(["symbol", "exDate_parsed"]):
        if pd.isna(date) or grp["ratio"].isna().all():
            continue
        ratios = grp["ratio"].dropna().tolist()
        combined = 1.0
        for r in ratios:
            combined *= r
        confirmed.setdefault(symbol, []).append({
            "date": date.strftime("%Y-%m-%d"),
            "combined_ratio": round(combined, 4),
            "actions": grp["subject"].str.strip().tolist(),
        })

    out_json = Path(__file__).parent / "confirmed_splits.json"
    out_json.write_text(json.dumps(confirmed, indent=2))
    print(f"Wrote {out_json} — {sum(len(v) for v in confirmed.values())} confirmed events "
          f"across {len(confirmed)} symbols.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch NSE corporate actions and derive confirmed split/bonus ratios")
    parser.add_argument("--from-date", default="01-01-2021", help="DD-MM-YYYY")
    parser.add_argument("--to-date", default="31-12-2026", help="DD-MM-YYYY")
    args = parser.parse_args()
    main(args.from_date, args.to_date)
