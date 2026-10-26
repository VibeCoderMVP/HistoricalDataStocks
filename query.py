"""
query.py — Query interface for HistoricalDataStocks.

Two data layers:
  daily()    — from data/daily/*.parquet  (one bar per trading day)
  intraday() — from data/5min/*.parquet   (5min raw or rolled up to any N-min TF)

For cross-symbol queries (universe scans), use universe_daily() which
uses DuckDB to read all Parquets in a single vectorised pass.

Usage examples:

    from query import daily, intraday, universe_daily, to_csv

    # Single symbol — daily bars
    df = daily("RELIANCE", from_date="2024-01-01")

    # Single symbol — 125min bars (rolled from 5min)
    df = intraday("RELIANCE", minutes=125, from_date="2025-01-01")

    # Full Nifty 200 daily — one DataFrame, symbol column identifies each row
    df = universe_daily(from_date="2024-01-01")

    # Export to CSV
    to_csv(df, "reliance_125min.csv")

    # Use in a backtest loop
    for symbol in ["RELIANCE", "INFY", "TCS"]:
        df = intraday(symbol, minutes=25)
        # ... run strategy on df ...
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from rollup import load_5min, rollup

DATA_DAILY = Path(__file__).parent / "data" / "daily"
DATA_5MIN  = Path(__file__).parent / "data" / "5min"

# Symbols whose daily parquet is known-corrupted and NOT worth fixing (see
# bug_hds_mothersonso_daily_duplicate_rows in project memory). Excluded from
# universe_daily() by default so a fresh caller can't silently re-ingest this
# and soil backtest results. Does NOT affect 5min/intraday stores, which don't
# have this bug for this symbol. daily("MOTHERSON") still works directly (with
# a warning) for anyone deliberately inspecting/fixing the raw file.
KNOWN_BAD_DAILY_SYMBOLS = {"MOTHERSON"}


# ── Daily TF ────────────────────────────────────────────────────────────────

def daily(
    symbol: str,
    from_date: str | None = None,
    to_date: str | None = None,
) -> pd.DataFrame:
    """Load daily OHLCV for one symbol. Index: date (tz-naive)."""
    path = DATA_DAILY / f"{symbol}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"No daily data for {symbol}. Run fetch_daily.py first.")
    if symbol in KNOWN_BAD_DAILY_SYMBOLS:
        print(f"WARNING: {symbol}'s daily parquet has known duplicate/corrupted rows "
              f"(see bug_hds_mothersonso_daily_duplicate_rows) — not fixed, use with caution.")
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")[["open", "high", "low", "close", "volume"]].sort_index()
    if from_date:
        df = df[from_date:]
    if to_date:
        df = df[:to_date]
    return df


def universe_daily(
    symbols: list[str] | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    include_known_bad: bool = False,
) -> pd.DataFrame:
    """
    Load daily data for the full Nifty 200 (or a subset) in one DuckDB pass.
    Returns a flat DataFrame with columns: symbol, date, open, high, low, close, volume.
    Faster than calling daily() in a loop for large universes.

    Excludes KNOWN_BAD_DAILY_SYMBOLS (currently just MOTHERSON — known duplicate/
    corrupted daily rows, not fixed, not worth fixing) by default. One fewer
    symbol out of 200 has no material effect on a universe-level backtest
    decision; leaving it in silently would. Pass include_known_bad=True to
    override (e.g. while working on the bug itself).
    """
    glob = str(DATA_DAILY / "*.parquet")
    where = []
    if symbols:
        sym_list = ", ".join(f"'{s}'" for s in symbols)
        where.append(f"symbol IN ({sym_list})")
    if not include_known_bad and KNOWN_BAD_DAILY_SYMBOLS:
        excl_list = ", ".join(f"'{s}'" for s in KNOWN_BAD_DAILY_SYMBOLS)
        where.append(f"symbol NOT IN ({excl_list})")
    if from_date:
        where.append(f"date >= '{from_date}'")
    if to_date:
        where.append(f"date <= '{to_date}'")

    sql = f"SELECT symbol, date, open, high, low, close, volume FROM read_parquet('{glob}')"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY symbol, date"

    return duckdb.query(sql).df()


# ── Intraday TF (5min base, roll to any N-min) ──────────────────────────────

def intraday(
    symbol: str,
    minutes: int = 5,
    from_date: str | None = None,
    to_date: str | None = None,
    adjusted: bool = False,
) -> pd.DataFrame:
    """
    Load intraday OHLCV for one symbol.
    minutes=5  → raw 5min bars (IST-indexed)
    minutes=N  → rolled up to N-min bars (N must be multiple of 5)
    Volume is always summed correctly in rollups.
    adjusted=True → OHLC put on the daily store's corporate-action-adjusted scale
    (intraday_adjust.py; fixes bug_hds_5min_daily_split_adjustment_mismatch). Default False
    keeps existing callers' behaviour unchanged. MOTHERSON is not adjustable — keep excluding it.
    """
    df = load_5min(symbol, from_date, to_date) if minutes == 5 else rollup(symbol, minutes, from_date, to_date)
    if adjusted:
        from intraday_adjust import apply_intraday_adjustment
        df = apply_intraday_adjustment(df, symbol=symbol, tf="5min")
    return df


# ── Export ───────────────────────────────────────────────────────────────────

def to_csv(df: pd.DataFrame, path: str) -> None:
    """Export any DataFrame to CSV."""
    df.to_csv(path)
    print(f"Exported {len(df)} rows to {path}")


# ── Convenience ──────────────────────────────────────────────────────────────

def available_symbols(tf: str = "daily") -> list[str]:
    """List symbols that have downloaded data. tf='daily' or '5min'."""
    folder = DATA_DAILY if tf == "daily" else DATA_5MIN
    return sorted(p.stem for p in folder.glob("*.parquet"))


if __name__ == "__main__":
    # Quick sanity check
    syms_d  = available_symbols("daily")
    syms_5m = available_symbols("5min")
    print(f"Daily:  {len(syms_d)} symbols")
    print(f"5min:   {len(syms_5m)} symbols")
    if syms_d:
        sample = syms_d[0]
        df = daily(sample)
        print(f"\n{sample} daily — {len(df)} bars, {df.index.min().date()} to {df.index.max().date()}")
        print(df.tail(3))
