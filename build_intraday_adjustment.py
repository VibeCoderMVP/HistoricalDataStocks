"""
build_intraday_adjustment.py — per-session price factors that put the intraday stores on the
daily store's corporate-action-adjusted scale (fix for bug_hds_5min_daily_split_adjustment_
mismatch, SEV1).

Why reconcile to the daily store rather than apply NSE ratios to "raw" intraday: measured
2026-10-26, the intraday parquet is NOT consistently raw. Dhan's intraday endpoint returned
already-adjusted bars for some symbols/events at fetch time (RELIANCE: 2024 bonus already in
5min prices) and unadjusted for others (360ONE: 2023 bonus missing). Applying NSE ratios would
double-adjust the former. The daily store IS consistently adjusted (incl. demergers — ITC
pre-demerger daily = 0.946 x intraday, matching the real -5.85% drop), so:

    factor(session) = daily_high / max(intraday high)   (cross-checked with lows)

Within a corporate-action segment the factor is constant to 4 decimals (SRF 1.0000, ITC
0.9460). The raw parquet is never modified — this writes a factor table; apply it on read with
intraday_adjust.apply_intraday_adjustment().

Daily dates are 1 calendar day early (bug_hds_daily_date_offset) — corrected (+1 day) here.

Sessions BEFORE the daily store's first date have no reference: for those only, the first
segment's factor is multiplied by NSE's confirmed split/bonus ratios (confirmed_splits.json)
for events between the session and the daily start (basis='nse_ratio_pre_daily'). Covers
SRF (Bonus 4:1, 2021-10-13) and APLAPOLLO (Bonus 1:1, 2021-09-16).

A symbol whose daily reference is itself noisy (any segment of >=20 sessions with p95
deviation > 1%, e.g. MOTHERSON's known-corrupt daily) is marked UNRELIABLE and left OUT of
the factor table — keep excluding it.

Outputs (per --tf):
  data/intraday_adjustment_<tf>.parquet          symbol, date, factor, basis (one row per session)
  data/intraday_adjustment_<tf>_segments.csv     audit: every constant-factor segment, its
                                                 dispersion, and the NSE event at its start

Usage:
    uv run python build_intraday_adjustment.py --tf 5min
    uv run python build_intraday_adjustment.py --tf 1min
"""

from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

DATA = Path(__file__).parent / "data"
PRICE_EVENTS = r"bonus|split|sub.?divi|consolidat|demerger|rights|arrangement|reduction"
STEP = 0.004       # a new segment needs a >0.4% move in factor ...
CONFIRM = 3        # ... held for 3 consecutive sessions (single-session glitches don't split)
EVENT_WINDOW = 5   # breakpoint "explained" if an NSE price event is within +-5 calendar days
UNRELIABLE_DEV_PCT = 1.0  # a segment (>=20 sessions) noisier than this = daily reference is bad


def session_ratios(symbol: str, tf: str) -> pd.DataFrame:
    intr = pd.read_parquet(DATA / tf / f"{symbol}.parquet", columns=["timestamp", "open", "high", "low", "close"])
    intr["date"] = pd.to_datetime(intr["timestamp"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata").dt.date
    s = intr.sort_values("timestamp").groupby("date").agg(
        ih=("high", "max"), il=("low", "min"), io=("open", "first"), ic=("close", "last"))
    s["raw_gap"] = s["io"] / s["ic"].shift(1) - 1   # overnight gap in the stored (unadjusted) bars

    daily = pd.read_parquet(DATA / "daily" / f"{symbol}.parquet", columns=["date", "high", "low"])
    daily["date"] = (pd.to_datetime(daily["date"]) + timedelta(days=1)).dt.date  # T-1 offset fix
    daily = daily.drop_duplicates("date", keep="last").set_index("date")

    m = s.join(daily, how="left")
    rh, rl = m["high"] / m["ih"], m["low"] / m["il"]
    # trust a session only when high- and low-based ratios agree (both from one clean factor)
    m["r"] = ((rh + rl) / 2).where((rh / rl - 1).abs() <= 0.002)
    return m


def segment(r: pd.Series) -> list[tuple[int, int, float]]:
    """Constant-factor segments over the per-session ratio series, as
    [(first_valid_pos, last_valid_pos, factor)]. Sessions with no trustworthy ratio (NaN) are
    not assigned here — see place_boundaries()."""
    vals = r.to_numpy()
    idx = [i for i in range(len(vals)) if pd.notna(vals[i])]
    if not idx:
        return []
    segs, first, last, cur = [], idx[0], idx[0], [vals[idx[0]]]
    for k in range(1, len(idx)):
        i = idx[k]
        med = float(pd.Series(cur).median())
        if abs(vals[i] / med - 1) > STEP:
            nxt = [vals[j] for j in idx[k:k + CONFIRM]]
            if len(nxt) == CONFIRM and all(abs(v / vals[i] - 1) <= STEP for v in nxt):
                segs.append((first, last, med))
                first, last, cur = i, i, [vals[i]]
            # else: isolated glitch, ignored for the median
            continue
        cur.append(vals[i])
        last = i
    segs.append((first, last, float(pd.Series(cur).median())))
    return segs


def place_boundaries(segs, dates, ex_dates) -> list[tuple[int, int, float]]:
    """Turn valid-session segments into contiguous ranges covering every session. Between two
    segments there can be sessions with no trustworthy ratio — typically the ex-date itself,
    where the special pre-open makes daily high/low differ from intraday. The boundary goes on
    the first session on/after an NSE ex-date inside that gap; with no event there, on the
    first unresolved session (an ex-day effect is the usual cause)."""
    out = []
    for k, (a, b, f) in enumerate(segs):
        start = 0 if k == 0 else out[-1][1] + 1
        if k + 1 < len(segs):
            nxt_first = segs[k + 1][0]
            end = b
            for p in range(b + 1, nxt_first + 1):
                if any(dates[b] < ex <= dates[p] for ex in ex_dates):
                    end = p - 1
                    break
            out.append((start, end, f))
        else:
            out.append((start, len(dates) - 1, f))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tf", choices=["5min", "1min"], default="5min")
    tf = ap.parse_args().tf

    ca = pd.read_parquet(DATA / "corporate_actions_raw.parquet", columns=["symbol", "exDate", "subject"])
    ca = ca[ca["subject"].str.contains(PRICE_EVENTS, case=False, na=False)]
    ca["ex"] = pd.to_datetime(ca["exDate"], format="%d-%b-%Y", errors="coerce").dt.date

    confirmed = json.loads((Path(__file__).parent / "confirmed_splits.json").read_text(encoding="utf-8"))

    factor_rows, seg_rows = [], []
    symbols = sorted(p.stem for p in (DATA / tf).glob("*.parquet"))
    for n, sym in enumerate(symbols, 1):
        if not (DATA / "daily" / f"{sym}.parquet").is_file():
            seg_rows.append({"symbol": sym, "note": "NO_DAILY — left unadjusted (factor 1.0)"})
            continue
        m = session_ratios(sym, tf)
        has_ref = m["high"].notna().to_numpy()
        first_ref = next((i for i in range(len(m)) if has_ref[i]), None)
        if first_ref is None:
            seg_rows.append({"symbol": sym, "note": "NO_DAILY_OVERLAP — left unadjusted"})
            continue
        dates = list(m.index)
        pre_dates, m = dates[:first_ref], m.iloc[first_ref:]   # no daily reference before here
        dates = dates[first_ref:]
        ex_dates = sorted(set(ca[ca["symbol"] == sym]["ex"].dropna()))
        segs = place_boundaries(segment(m["r"]), dates, ex_dates)
        if not segs:
            seg_rows.append({"symbol": sym, "note": "NO_TRUSTWORTHY_RATIO — left unadjusted"})
            continue
        sym_rows, sym_segs = [], []
        # Pre-daily window: apply an NSE ratio only where the stored bars actually show the
        # unadjusted jump at that ex-date (POWERGRID's 2021 bonus was already adjusted by Dhan).
        all_gaps = session_ratios(sym, tf)["raw_gap"]
        applied, skipped = [], []
        for e in confirmed.get(sym, []):
            ex, ratio = date.fromisoformat(e["date"]), e["combined_ratio"]
            if not (pre_dates and pre_dates[0] < ex <= dates[0]):
                continue
            day = next((d for d in all_gaps.index if d >= ex), None)
            gap = all_gaps.get(day) if day is not None else None
            (applied if gap is not None and abs(gap - (ratio - 1)) <= 0.06 else skipped).append((ex, ratio, gap))
        for d in pre_dates:
            f = segs[0][2]
            for ex, ratio, _ in applied:
                if d < ex:
                    f *= ratio
            sym_rows.append((sym, d, round(f, 5), "nse_ratio_pre_daily" if applied else "daily_first_segment"))
        if pre_dates:
            sym_segs.append({"symbol": sym, "segment": -1, "start": pre_dates[0], "end": pre_dates[-1],
                             "sessions": len(pre_dates), "factor": sym_rows[0][2], "p95_dev_pct": None,
                             "nse_event_at_start": "pre-daily window; applied: "
                             + "; ".join(f"{ex} x{r} (raw gap {g:+.1%})" for ex, r, g in applied)
                             + (" | skipped (raw already adjusted): "
                                + "; ".join(f"{ex} x{r} (raw gap {g:+.1%})" if g is not None else f"{ex} x{r}"
                                            for ex, r, g in skipped) if skipped else ""),
                             "note": "PRE_DAILY_WINDOW"})
        for si, (a, b, f) in enumerate(segs):
            part = m["r"].iloc[a:b + 1].dropna()
            disp = float((part / f - 1).abs().quantile(0.95)) if len(part) else None
            start = dates[a]
            ev = ca[(ca["symbol"] == sym) & ca["ex"].between(start - timedelta(days=EVENT_WINDOW),
                                                               start + timedelta(days=EVENT_WINDOW))]
            sym_segs.append({
                "symbol": sym, "segment": si, "start": start, "end": dates[b], "sessions": b - a + 1,
                "factor": round(f, 5), "p95_dev_pct": None if disp is None else round(100 * disp, 3),
                "nse_event_at_start": "; ".join(ev["subject"].str.strip()) if si > 0 else "",
                "note": "" if si == 0 or len(ev) else "UNEXPLAINED_BREAK",
            })
            for d in dates[a:b + 1]:
                sym_rows.append((sym, d, round(f, 5), "daily"))
        unreliable = any((g.get("sessions") or 0) >= 20 and (g.get("p95_dev_pct") or 0) > UNRELIABLE_DEV_PCT
                         for g in sym_segs)
        if unreliable:
            for g in sym_segs:
                g["note"] = "UNRELIABLE (noisy daily reference) — NOT in factor table, keep excluded"
        else:
            factor_rows.extend(sym_rows)
        seg_rows.extend(sym_segs)
        if n % 50 == 0:
            print(f"  {n}/{len(symbols)}")

    fac = pd.DataFrame(factor_rows, columns=["symbol", "date", "factor", "basis"])
    fac.to_parquet(DATA / f"intraday_adjustment_{tf}.parquet", index=False)
    segs = pd.DataFrame(seg_rows)
    segs.to_csv(DATA / f"intraday_adjustment_{tf}_segments.csv", index=False)

    adjusted = segs[(segs.get("factor", 1.0) - 1).abs() > 0.0005]["symbol"].unique() if "factor" in segs else []
    multi = segs[segs["segment"].fillna(0) >= 0].groupby("symbol").size()
    print(f"\n{tf}: {len(symbols)} symbols, {len(fac)} session factors")
    print(f"symbols with any factor != 1: {len(adjusted)} -> {sorted(adjusted)}")
    print(f"symbols with >1 segment: {int((multi > 1).sum())}")
    print(f"unexplained breaks: {int((segs['note'] == 'UNEXPLAINED_BREAK').sum())}")
    notes = segs["note"].fillna("")
    print(f"UNRELIABLE (excluded): {sorted(segs[notes.str.startswith('UNRELIABLE')]['symbol'].unique())}")
    pre = segs[notes == "PRE_DAILY_WINDOW"]
    print(f"pre-daily windows with an NSE ratio applied: "
          f"{sorted(pre[pre['nse_event_at_start'].str.contains('applied: 2', na=False)]['symbol'])}")
    noisy = segs[segs["p95_dev_pct"].fillna(0) > 0.3] if "p95_dev_pct" in segs else segs.iloc[0:0]
    print(f"noisy segments (p95 dev > 0.3%): {len(noisy)}")


if __name__ == "__main__":
    main()
