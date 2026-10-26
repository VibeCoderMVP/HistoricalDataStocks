#!/usr/bin/env python3
"""
Pull historical quarterly-results announcement dates for the Nifty-200 universe from NSE's
public corporate-announcements API (https://www.nseindia.com/api/corporate-announcements).

Not a Dhan API -- Dhan's SDK (checked 2026-08-09, see memory) exposes no earnings-calendar
endpoint. NSE's own corporate-filings API does carry this, but the `desc` CATEGORY LABEL for
a results filing is NOT stable over time -- confirmed 2026-08-09 by comparing two real TCS
filings: 2023-04-12 used `desc == "Financial Result Updates"`, 2026-07-09 used
`desc == "Outcome of Board Meeting"`. An earlier version of this script filtered on
desc=="Outcome of Board Meeting", which silently missed almost all pre-2025 history (only 35-98
rows/year found for 2021-2024 vs 445-525 for 2025-2026 -- a red flag caught by a per-symbol
sanity check, not assumed clean). Fixed: match on the boilerplate PHRASE inside `attchmntText`
instead of the unstable `desc` field -- "financial results for the period ended" appears
verbatim in both the 2023 and 2026 examples regardless of category label, and is NSE's own
auto-generated wording for this specific filing type.

The API silently caps how much history a single from_date/to_date call actually returns
(observed: a 6-month request returned only the most recent ~2 months of it) -- so this script
paginates in ~45-day windows across the target range instead of trusting a wide single call.

Requires verify=False (this environment's CA trust store doesn't chain NSE's cert -- a local
environment issue, not an NSE/security concern; NSE announcement data is public, unauthenticated,
no credentials involved). Checkpointed/resumable -- safe to re-run after an interruption.

Usage:
    D:\\Trading\\HistoricalDataStocks\\.venv\\Scripts\\python.exe fetch_earnings_dates.py
"""
from __future__ import annotations

import datetime as dt
import time
from pathlib import Path

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SYMBOLS_FILE = Path(r"D:\Trading\HistoricalDataStocks\symbols\nifty200.txt")
OUT_DIR = Path(r"D:\Trading\HistoricalDataStocks\data")
CHECKPOINT_PATH = OUT_DIR / "earnings_dates_checkpoint.csv"
OUT_PATH = OUT_DIR / "earnings_dates.csv"

START_DATE = dt.date(2021, 1, 1)
END_DATE = dt.date.today()
WINDOW_DAYS = 45  # comfortably under the API's observed ~60-day effective cap

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*,q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}


def load_universe() -> set[str]:
    return {s.strip().upper() for s in SYMBOLS_FILE.read_text(encoding="utf-8").splitlines() if s.strip()}


def make_session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    s.get("http://nseindia.com", headers=HEADERS, timeout=15)
    return s


def date_windows(start: dt.date, end: dt.date, window_days: int) -> list[tuple[dt.date, dt.date]]:
    windows = []
    cur = start
    while cur <= end:
        win_end = min(cur + dt.timedelta(days=window_days - 1), end)
        windows.append((cur, win_end))
        cur = win_end + dt.timedelta(days=1)
    return windows


def fetch_window(session: requests.Session, start: dt.date, end: dt.date) -> pd.DataFrame:
    ref_url = "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
    ref = session.get(ref_url, headers=HEADERS, timeout=15)
    url = (
        "https://www.nseindia.com/api/corporate-announcements?index=equities"
        f"&from_date={start.strftime('%d-%m-%Y')}&to_date={end.strftime('%d-%m-%Y')}"
    )
    resp = session.get(url, headers=HEADERS, cookies=ref.cookies.get_dict(), timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)


def main():
    universe = load_universe()
    windows = date_windows(START_DATE, END_DATE, WINDOW_DAYS)

    done_windows: set[str] = set()
    rows: list[dict] = []
    if CHECKPOINT_PATH.exists():
        prior = pd.read_csv(CHECKPOINT_PATH)
        rows = prior.to_dict("records")
        done_windows = set(prior["_window"].unique()) if "_window" in prior.columns else set()
        print(f"[resume] {len(done_windows)} windows already done, {len(rows)} rows loaded")

    session = make_session()

    for i, (start, end) in enumerate(windows, start=1):
        win_key = f"{start.isoformat()}_{end.isoformat()}"
        if win_key in done_windows:
            continue
        try:
            df = fetch_window(session, start, end)
        except Exception as e:
            print(f"  WARNING: window {win_key} failed: {e}")
            time.sleep(3)
            continue

        if not df.empty and "symbol" in df.columns and "attchmntText" in df.columns:
            df["symbol"] = df["symbol"].astype(str).str.upper()
            mask = (
                df["symbol"].isin(universe)
                & df["attchmntText"].str.contains("financial results for the period ended", case=False, na=False)
            )
            hits = df.loc[mask, ["symbol", "an_dt", "desc", "attchmntText"]].copy()
            for _, r in hits.iterrows():
                rows.append(dict(
                    symbol=r["symbol"],
                    announcement_datetime=r["an_dt"],
                    desc=r["desc"],
                    attchmntText=r["attchmntText"],
                    _window=win_key,
                ))

        done_windows.add(win_key)
        pd.DataFrame(rows).to_csv(CHECKPOINT_PATH, index=False)

        if i % 5 == 0 or i == len(windows):
            print(f"  [{i}/{len(windows)}] {win_key} -> {len(rows)} earnings rows so far")
        time.sleep(1.0)  # polite pacing against NSE's public API

    final = pd.DataFrame(rows)
    if not final.empty:
        final = final.drop(columns=["_window"])
        final["announcement_datetime"] = pd.to_datetime(final["announcement_datetime"], format="%d-%b-%Y %H:%M:%S")
        final["result_date"] = final["announcement_datetime"].dt.date
        final = final.drop_duplicates(subset=["symbol", "result_date"]).sort_values(["symbol", "result_date"])
    final.to_csv(OUT_PATH, index=False)
    print(f"\nDone: {len(final)} unique symbol/result-date rows -> {OUT_PATH}")
    print(f"Symbols with at least one result found: {final['symbol'].nunique() if not final.empty else 0} / {len(universe)}")


if __name__ == "__main__":
    main()
