"""
rollup.py — Resample 5min OHLCV bars to any N-minute timeframe.

Key rules:
  O = first bar's open
  H = max of all highs
  L = min of all lows
  C = last bar's close
  V = SUM of all volumes  ← volume-based indicators require this

Bin alignment anchored to 09:15 IST (NSE market open). This means:
  15min  → 09:15, 09:30, 09:45 ...
  25min  → 09:15, 09:40, 10:05 ...
  75min  → 09:15, 10:30, 11:45, 13:00, 14:15, 15:30
  125min → 09:15, 11:20, 13:25  (3 bars/day)

Only market-hours bars are included (09:15 to 15:30 IST). Pre/post-market filtered out.

Usage:
    from rollup import rollup, load_5min

    # Load raw 5min bars as IST-indexed DataFrame
    df5 = load_5min("RELIANCE", from_date="2025-01-01")

    # Roll up to any TF
    df25  = rollup("RELIANCE", minutes=25)
    df125 = rollup("RELIANCE", minutes=125, from_date="2024-01-01", to_date="2024-12-31")
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

DATA_5MIN = Path(__file__).parent / "data" / "5min"


def load_5min(
    symbol: str,
    from_date: str | None = None,
    to_date: str | None = None,
) -> pd.DataFrame:
    """Load 5min Parquet as IST-indexed OHLCV DataFrame."""
    path = DATA_5MIN / f"{symbol}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"No 5min data for {symbol}. Run fetch_5min.py first.")
    df = pd.read_parquet(path)
    df["datetime"] = (
        pd.to_datetime(df["timestamp"], unit="s", utc=True)
        .dt.tz_convert("Asia/Kolkata")
    )
    df = df.set_index("datetime")[["open", "high", "low", "close", "volume"]].sort_index()
    # Market hours filter
    df = df.between_time("09:15", "15:30")
    if from_date:
        df = df[from_date:]
    if to_date:
        df = df[:to_date]
    return df


def rollup(
    symbol: str,
    minutes: int,
    from_date: str | None = None,
    to_date: str | None = None,
) -> pd.DataFrame:
    """
    Resample 5min bars to an N-minute TF.

    minutes must be a multiple of 5 (5, 10, 15, 25, 30, 75, 125, ...).
    Volume is summed — critical for VWAP, VWMA, OBV, and volume-ratio indicators.
    """
    if minutes % 5 != 0:
        raise ValueError(f"minutes must be a multiple of 5, got {minutes}")

    df = load_5min(symbol, from_date, to_date)

    # Session-based binning: anchor each day independently to 09:15 IST.
    # pandas resample(origin=...) drifts for non-factor-of-1440 intervals (e.g. 125min),
    # because 1440 % 125 != 0. Session grouping is the correct approach.
    work = df.copy()
    work["_date"] = work.index.date
    work["_mso"]  = work.index.hour * 60 + work.index.minute - 9 * 60 - 15  # minutes since 09:15
    work["_bin"]  = work["_mso"] // minutes

    grouped = work.groupby(["_date", "_bin"])
    agg = grouped.agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    ).reset_index()

    # Reconstruct IST timestamps: date 09:15 + bin * minutes
    agg["datetime"] = (
        pd.to_datetime(agg["_date"].astype(str))
        + pd.to_timedelta("9h15min")
        + pd.to_timedelta(agg["_bin"] * minutes, unit="min")
    )
    agg["datetime"] = agg["datetime"].dt.tz_localize("Asia/Kolkata")
    resampled = agg.set_index("datetime")[["open", "high", "low", "close", "volume"]].sort_index()
    resampled["symbol"] = symbol
    return resampled


def available_symbols() -> list[str]:
    return sorted(p.stem for p in DATA_5MIN.glob("*.parquet"))
