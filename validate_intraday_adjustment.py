"""
validate_intraday_adjustment.py — prove the intraday factor table fixes the split mismatch.

Checks, all 200 symbols:
  1. Every NSE price event (bonus/split/demerger/rights/...) inside the intraday range:
     overnight gap raw vs adjusted. Adjusted |gap| must be <= 8% (or match a real market move
     — Kite's independently adjusted gap is shown for those 16 symbols where it was fetched).
  2. The original bug's scan: per-session ratio daily_close / last intraday close, adjusted.
     Spread (max/min of per-quarter medians) must be < 1.02 — the bug flagged > 1.3.
  3. Rollup consistency: a 125min rollup of adjusted 5min == adjusting the precomputed 125min.

Usage:
    .venv/Scripts/python.exe validate_intraday_adjustment.py --tf 5min
"""

from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path

import pandas as pd

from build_intraday_adjustment import PRICE_EVENTS
from intraday_adjust import apply_intraday_adjustment

DATA = Path(__file__).parent / "data"
KITE_CONT = Path(__file__).parent.parent / "HistoricalDataKite" / "data" / "corp_action_continuity.csv"


def sessions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(date=pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata").dt.date)
    g = df.sort_values("timestamp").groupby("date")
    s = pd.DataFrame({"open": g["open"].first(), "close": g["close"].last()})
    s["gap_pct"] = 100 * (s["open"] / s["close"].shift(1) - 1)
    return s


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tf", choices=["5min", "1min"], default="5min")
    tf = ap.parse_args().tf

    fac = pd.read_parquet(DATA / f"intraday_adjustment_{tf}.parquet")
    covered = set(fac["symbol"])
    ca = pd.read_parquet(DATA / "corporate_actions_raw.parquet", columns=["symbol", "exDate", "subject"])
    ca = ca[ca["subject"].str.contains(PRICE_EVENTS, case=False, na=False)]
    ca["ex"] = pd.to_datetime(ca["exDate"], format="%d-%b-%Y", errors="coerce").dt.date
    kite = pd.read_csv(KITE_CONT) if KITE_CONT.is_file() else pd.DataFrame(columns=["symbol", "date", "kite_gap_pct"])
    kite["date"] = pd.to_datetime(kite["date"]).dt.date

    ev_rows, scan_rows = [], []
    for path in sorted((DATA / tf).glob("*.parquet")):
        sym = path.stem
        raw = pd.read_parquet(path)
        adj = apply_intraday_adjustment(raw, symbol=sym, tf=tf)
        rs, as_ = sessions(raw), sessions(adj)
        for _, e in ca[ca["symbol"] == sym].drop_duplicates("ex").iterrows():
            day = next((d for d in as_.index if d >= e["ex"]), None)
            if day is None or day == as_.index[0] or e["ex"] < as_.index[0]:
                continue
            k = kite[(kite["symbol"] == sym) & (kite["date"] == day)]["kite_gap_pct"]
            ev_rows.append({"symbol": sym, "ex": day, "subject": e["subject"].strip()[:40],
                            "raw_gap": round(rs.loc[day, "gap_pct"], 2), "adj_gap": round(as_.loc[day, "gap_pct"], 2),
                            "kite_gap": round(float(k.iloc[0]), 2) if len(k) else None,
                            "in_table": sym in covered})
        daily = pd.read_parquet(DATA / "daily" / f"{sym}.parquet", columns=["date", "close"])
        daily["date"] = (pd.to_datetime(daily["date"]) + timedelta(days=1)).dt.date
        daily = daily.drop_duplicates("date", keep="last").set_index("date")["close"]
        for label, s in (("raw", rs), ("adj", as_)):
            r = (daily.reindex(s.index) / s["close"]).dropna()
            q = r.groupby(pd.to_datetime(pd.Series(r.index, index=r.index)).dt.to_period("Q")).median()
            scan_rows.append({"symbol": sym, "kind": label, "spread": float(q.max() / q.min()) if len(q) else None})

    ev = pd.DataFrame(ev_rows)
    ev["verdict"] = ev["adj_gap"].abs().map(lambda a: "OK" if a <= 8 else ("CHECK" if a <= 15 else "BROKEN"))
    ev.to_csv(DATA / f"intraday_adjustment_{tf}_validation.csv", index=False)
    pd.set_option("display.width", 220)
    print(f"== 1. NSE price events inside {tf} range: {len(ev)}")
    print(ev.groupby(["in_table", "verdict"]).size().to_string())
    bad = ev[ev["verdict"] != "OK"]
    if len(bad):
        print(bad.to_string(index=False))

    sc = pd.DataFrame(scan_rows).pivot(index="symbol", columns="kind", values="spread")
    print("\n== 2. daily/intraday quarterly-ratio spread (bug threshold 1.3)")
    print(f"raw: {(sc['raw'] > 1.3).sum()} symbols > 1.3, {(sc['raw'] > 1.02).sum()} > 1.02")
    print(f"adj: {(sc['adj'] > 1.3).sum()} symbols > 1.3, {(sc['adj'] > 1.02).sum()} > 1.02")
    worst = sc.sort_values("adj", ascending=False).head(6)
    print(worst.round(4).to_string())

    if tf == "5min" and (DATA / "125min" / "360ONE.parquet").is_file():
        import sys
        sys.path.insert(0, str(Path(__file__).parent))
        from rollup import rollup  # noqa: E402
        r1 = apply_intraday_adjustment(rollup("360ONE", 125), symbol="360ONE", tf=tf).reset_index()
        r1 = r1.rename(columns={r1.columns[0]: "datetime"}) if "datetime" not in r1.columns else r1
        pre = apply_intraday_adjustment(pd.read_parquet(DATA / "125min" / "360ONE.parquet"), symbol="360ONE", tf=tf)
        m = r1.assign(datetime=pd.to_datetime(r1["datetime"])).merge(
            pre.assign(datetime=pd.to_datetime(pre["datetime"])), on="datetime", suffixes=("_r", "_p"))
        print(f"\n== 3. 360ONE 125min: rollup-then-adjust vs precomputed-then-adjust on {len(m)} common bars, "
              f"max |close diff| = {(m['close_r'] - m['close_p']).abs().max():.4f}")

if __name__ == "__main__":
    main()
