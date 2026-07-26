"""
detect_split_candidates.py — Flag likely stock split / bonus events in the daily
OHLCV store, since data/daily/*.parquet carries NO corporate-action adjustment
(see CLAUDE.md "Known Limitations").

Heuristic (price-ratio based, not just "big single-day drop"):
  1. Compute close-to-close overnight return for every symbol/day.
  2. Flag days where |return| >= MOVE_THRESHOLD (default 12%).
  3. Among flagged days, check whether close[t]/close[t-1] lands close to a
     "clean" split/bonus ratio (2:1, 3:1, 5:1, 10:1, bonus 1:1, 1:2, 1:3, 1:5, ...).
     A real split/bonus lands almost exactly on one of these; a news-driven
     circuit-limit crash almost never does — this is what separates a split
     candidate from a real crash, not the move size alone.
  4. Cross-check against NIFTY 50 same-day move (data/index_daily/NIFTY50.parquet).
     A large single-stock move on a day the index barely moved is further
     evidence it's stock-specific (split) rather than market-wide.

Output:
  - split_candidates.csv   — every flagged day, ratio match, confidence, index move
  - split_candidates.json  — {symbol: [dates]} for HIGH-confidence matches only,
    ready to feed into a backtest as an exclude/ignore window (±1 day either
    side, since Dhan's daily date column is already known to carry a T-1 shift
    — see fetch_daily.py docstring).

This is a heuristic first pass, not authoritative. For anything precision-
critical, cross-check flagged symbols against NSE's actual corporate-action
archive (bhavcopy corporate-action file, same source BhavCopy/ already pulls
from) before trusting the ratio blindly.

Usage:
    uv run python detect_split_candidates.py
    uv run python detect_split_candidates.py --threshold 0.10 --tolerance 0.03
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

DATA_DAILY = Path(__file__).parent / "data" / "daily"
DATA_INDEX = Path(__file__).parent / "data" / "index_daily" / "NIFTY50.parquet"

# ratio = close[t] / close[t-1] for a clean N:M split or bonus issue
CLEAN_RATIOS: dict[str, float] = {
    "2:1 split / 1:1 bonus":  0.500,
    "3:1 split / 2:1 bonus":  0.333,
    "3:2 split / 1:2 bonus":  0.667,
    "4:1 split / 3:1 bonus":  0.250,
    "5:1 split / 4:1 bonus":  0.200,
    "10:1 split / 9:1 bonus": 0.100,
    "1:3 bonus":              0.750,
    "1:4 bonus":              0.800,
    "1:5 bonus":              0.833,
    "1:10 bonus":             0.909,
}


def classify_ratio(ratio: float, tolerance: float) -> tuple[str | None, float]:
    """Return (best matching label, abs distance) or (None, min distance) if no match within tolerance."""
    best_label, best_dist = None, float("inf")
    for label, clean in CLEAN_RATIOS.items():
        dist = abs(ratio - clean)
        if dist < best_dist:
            best_label, best_dist = label, dist
    if best_dist <= tolerance:
        return best_label, best_dist
    return None, best_dist


def main(threshold: float, tolerance: float) -> None:
    if not DATA_INDEX.exists():
        raise FileNotFoundError(f"{DATA_INDEX} missing — run fetch_index_daily.py first.")
    nifty = pd.read_parquet(DATA_INDEX)
    nifty["date"] = pd.to_datetime(nifty["date"])
    nifty = nifty.set_index("date")["close"].sort_index()
    nifty_ret = nifty.pct_change()

    rows = []
    files = sorted(DATA_DAILY.glob("*.parquet"))
    for f in files:
        symbol = f.stem
        df = pd.read_parquet(f)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
        df["ret"] = df["close"].pct_change()
        df["ratio"] = df["close"] / df["close"].shift(1)

        flagged = df[df["ret"].abs() >= threshold]
        for _, r in flagged.iterrows():
            label, dist = classify_ratio(r["ratio"], tolerance)
            idx_ret = nifty_ret.get(r["date"], float("nan"))
            confidence = "HIGH" if label is not None else "LOW"
            rows.append({
                "symbol": symbol,
                "date": r["date"].date(),
                "prev_close": round(df.loc[r.name - 1, "close"], 2) if r.name > 0 else None,
                "close": round(r["close"], 2),
                "pct_change": round(r["ret"] * 100, 2),
                "ratio_match": label or "no clean match",
                "ratio_distance": round(dist, 4),
                "nifty50_pct_change": round(idx_ret * 100, 2) if pd.notna(idx_ret) else None,
                "confidence": confidence,
            })

    if not rows:
        print(f"No days found with |move| >= {threshold:.0%}.")
        return

    report = pd.DataFrame(rows).sort_values(["confidence", "symbol", "date"], ascending=[True, True, True])

    # Cascade filter: a real split/bonus is symbol-specific. If N or more
    # *different* symbols all land a HIGH-confidence "clean ratio" match on the
    # same calendar date, that's a market-wide event (crash, circuit cascade,
    # election/budget-day move) coincidentally landing near a clean fraction —
    # not N independent corporate actions on one day. Demote those to LOW.
    CASCADE_MIN_SYMBOLS = 3
    high_mask = report["confidence"] == "HIGH"
    symbols_per_date = report[high_mask].groupby("date")["symbol"].nunique()
    cascade_dates = set(symbols_per_date[symbols_per_date >= CASCADE_MIN_SYMBOLS].index)
    cascade_mask = high_mask & report["date"].isin(cascade_dates)
    report.loc[cascade_mask, "confidence"] = "LOW (cascade — market-wide move, not a split)"

    out_csv = Path(__file__).parent / "split_candidates.csv"
    report.to_csv(out_csv, index=False)

    high = report[report["confidence"] == "HIGH"]
    exclude_map: dict[str, list[str]] = {}
    for symbol, grp in high.groupby("symbol"):
        exclude_map[symbol] = sorted(str(d) for d in grp["date"])
    out_json = Path(__file__).parent / "split_candidates.json"
    out_json.write_text(json.dumps(exclude_map, indent=2))

    print(f"Scanned {len(files)} symbols.")
    print(f"Flagged {len(report)} days with |move| >= {threshold:.0%}.")
    print(f"  HIGH confidence (clean ratio, symbol-specific): {len(high)} days across {len(exclude_map)} symbols")
    print(report[report['confidence'] != 'HIGH']['confidence'].value_counts().to_string())
    print(f"\nWrote {out_csv}")
    print(f"Wrote {out_json}  (HIGH-confidence symbol->dates, for backtest exclusion)")

    if len(high):
        print("\nHIGH-confidence candidates:")
        print(high[["symbol", "date", "pct_change", "ratio_match", "nifty50_pct_change"]].to_string(index=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detect stock split/bonus candidates in daily OHLCV store")
    parser.add_argument("--threshold", type=float, default=0.12, help="Min abs overnight return to flag (default 0.12 = 12%%)")
    parser.add_argument("--tolerance", type=float, default=0.025, help="Max distance from a clean ratio to count as HIGH confidence (default 0.025)")
    args = parser.parse_args()
    main(threshold=args.threshold, tolerance=args.tolerance)
