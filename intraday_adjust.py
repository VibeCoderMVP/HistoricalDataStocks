"""
intraday_adjust.py — apply corporate-action factors to HDS intraday bars on read.

The raw intraday parquet (data/5min, data/1min and TFs rolled from them) is NOT on the same
price scale as data/daily across splits/bonuses/demergers for some symbols (SEV1
bug_hds_5min_daily_split_adjustment_mismatch). build_intraday_adjustment.py derives a
per-session factor; this multiplies OHLC by it. Factors are constant within a session, so
this is valid for 5min, 1min and any session-anchored rollup (15/25/75/125min ...).

Volume is left as stored: the daily store's volume is not share-count adjusted either, and
Dhan's intraday volume is adjusted for some events and not others. Don't compare volume
levels across a split/bonus date.

Usage from any project (no import of HDS's package needed):
    import sys; sys.path.insert(0, r"D:\\Trading\\HistoricalDataStocks")
    from intraday_adjust import apply_intraday_adjustment
    df = apply_intraday_adjustment(df, symbol="360ONE", tf="5min")

`df` needs either an epoch-seconds `timestamp` column, a tz-aware datetime column/index, or a
`date` column; plus `symbol` when the frame holds several symbols and `symbol=` isn't given.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import pandas as pd

DATA = Path(__file__).parent / "data"
PRICE_COLS = ["open", "high", "low", "close"]


@lru_cache(maxsize=2)
def _factors(tf: str) -> pd.DataFrame:
    path = DATA / f"intraday_adjustment_{tf}.parquet"
    if not path.is_file():
        raise FileNotFoundError(f"{path} missing — run build_intraday_adjustment.py --tf {tf}")
    return pd.read_parquet(path)


def _session_dates(df: pd.DataFrame) -> pd.Series:
    if "timestamp" in df.columns:
        ts = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata")
        return ts.dt.date
    for col in ("ts", "datetime", "time"):
        if col in df.columns:
            ts = pd.to_datetime(df[col])
            ts = ts.dt.tz_convert("Asia/Kolkata") if ts.dt.tz is not None else ts
            return ts.dt.date
    if isinstance(df.index, pd.DatetimeIndex):
        idx = df.index.tz_convert("Asia/Kolkata") if df.index.tz is not None else df.index
        return pd.Series(idx.date, index=df.index)
    if "date" in df.columns:
        return pd.to_datetime(df["date"]).dt.date
    raise ValueError("can't find a timestamp/datetime/date to key the session factor on")


def apply_intraday_adjustment(df: pd.DataFrame, symbol: str | None = None, tf: str = "5min") -> pd.DataFrame:
    """Return a copy of df with OHLC multiplied by the session factor (1.0 where none)."""
    if df.empty:
        return df.copy()
    fac = _factors(tf)
    out = df.copy()
    dates = _session_dates(out).to_numpy()
    syms = [symbol] * len(out) if symbol is not None else out["symbol"].to_numpy()
    key = pd.MultiIndex.from_arrays([syms, dates])
    lookup = fac.set_index(["symbol", "date"])["factor"]
    f = lookup.reindex(key).to_numpy()
    # sessions newer than the factor table (fetched after the last build) sit on the current
    # scale -> 1.0 is correct until the next corporate action, when the table must be rebuilt
    f = pd.Series(f).fillna(1.0).to_numpy()
    for c in PRICE_COLS:
        if c in out.columns:
            out[c] = out[c] * f
    return out
