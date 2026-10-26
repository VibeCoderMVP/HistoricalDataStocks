"""
fetch_daily.py — Download 5 years of daily OHLCV for Nifty 200 from Dhan.

One API call per symbol (Dhan allows 5yr in a single call).
Saves to data/daily/{SYMBOL}.parquet.

Usage:
    uv run python fetch_daily.py                  # all Nifty 200
    uv run python fetch_daily.py --symbol RELIANCE # single symbol
    uv run python fetch_daily.py --years 3        # shorter history
    uv run python fetch_daily.py --force          # re-fetch even if file exists

Rate: 4 calls/sec (Dhan limit is 5/sec; stay conservative).
Time: ~200 symbols * 0.25s = ~1 minute for full Nifty 200.

NOTE on Dhan date shift: Dhan's daily data API returns OHLCV for trading date T
under a timestamp label of T-1 (previous trading day). The stored date column
has this off-by-one. For purely technical backtests (no event-date matching),
this is harmless — all bars have the same shift. For event-driven work, apply
+1 trading day correction. See CLAUDE.md for details.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import truststore

truststore.inject_into_ssl()

from dhanhq import DhanContext, dhanhq
from dotenv import load_dotenv

from instruments import NIFTY_200, NIFTY_500

DATA_DIR = Path(__file__).parent / "data" / "daily"
ENV_PATH = Path("D:/Trading/TradingWebSockets/.env")
SLEEP = 0.25  # 4 req/sec — stays comfortably under 5/sec rate limit


def _client() -> dhanhq:
    load_dotenv(dotenv_path=ENV_PATH)
    ctx = DhanContext(os.environ["DHAN_CLIENT_ID"], os.environ["DHAN_ACCESS_TOKEN"])
    return dhanhq(ctx)


class DhanNoDataAccess(RuntimeError):
    """DH-902: the account isn't entitled to Data APIs (e.g. "requires a PLUS subscription.
    Current plan: FREE"). Account-level and permanent for this run, so every symbol would
    fail identically -- retrying wastes ~10 min (bug_ingest_daily_bars_dhan_data_api_subscription_lapsed)."""


def _is_dh902(r: dict) -> bool:
    remarks = r.get("remarks")
    if isinstance(remarks, dict):
        return remarks.get("error_code") == "DH-902"
    return "DH-902" in str(remarks) or "DH-902" in str(r.get("data"))


def fetch_one(
    client: dhanhq, symbol: str, security_id: str, years: int = 5, retries: int = 3
) -> pd.DataFrame | None:
    today = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=years * 365)).strftime("%Y-%m-%d")

    r = None
    for attempt in range(1, retries + 1):
        r = client.historical_daily_data(
            security_id=security_id,
            exchange_segment="NSE_EQ",
            instrument_type="EQUITY",
            from_date=from_date,
            to_date=today,
            expiry_code=0,
        )
        if r.get("status") == "success" and isinstance(r.get("data"), dict):
            break
        if _is_dh902(r):
            raise DhanNoDataAccess(str(r.get("remarks") or r.get("data")))
        if attempt < retries:
            print(f"    {symbol}: attempt {attempt}/{retries} failed "
                  f"({r.get('remarks') or r.get('data')}) — retrying...")
            time.sleep(1.0)

    if r.get("status") != "success" or not isinstance(r.get("data"), dict):
        print(f"    {symbol}: giving up after {retries} attempts "
              f"({r.get('remarks') or r.get('data')})")
        return None
    data = r["data"]
    df = pd.DataFrame({
        "date":   pd.to_datetime(data["timestamp"], unit="s").normalize(),
        "open":   data["open"],
        "high":   data["high"],
        "low":    data["low"],
        "close":  data["close"],
        "volume": data["volume"],
    })
    df["symbol"] = symbol
    return df[["symbol", "date", "open", "high", "low", "close", "volume"]].sort_values("date").reset_index(drop=True)


def main(symbols: dict[str, str] | None = None, years: int = 5, force: bool = False) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    client = _client()
    universe = symbols or NIFTY_200
    total = len(universe)
    failed = []

    for i, (symbol, sec_id) in enumerate(universe.items(), 1):
        out = DATA_DIR / f"{symbol}.parquet"
        if out.exists() and not force:
            print(f"[{i:3d}/{total}] {symbol:16s} — exists, skip  (--force to refetch)")
            continue
        try:
            df = fetch_one(client, symbol, sec_id, years)
        except DhanNoDataAccess as e:
            print(f"[{i:3d}/{total}] {symbol:16s} — DH-902, aborting the Dhan fetch for ALL symbols:")
            print(f"    {e}")
            print("    Account has no Dhan Data API access -- every symbol would fail the same way.")
            print("    Check the plan at Dhan Web > My Profile > DhanHQ APIs. 0 files updated;")
            print("    the bhavcopy step still provides today's (split-UNadjusted) daily bars.")
            sys.exit(2)
        if df is not None and not df.empty:
            df.to_parquet(out, index=False)
            print(f"[{i:3d}/{total}] {symbol:16s} - {len(df):4d} bars  ({df['date'].min().date()} to {df['date'].max().date()})")
        else:
            print(f"[{i:3d}/{total}] {symbol:16s} — FAILED")
            failed.append(symbol)
        time.sleep(SLEEP)

    print(f"\nDone. {total - len(failed)}/{total} symbols saved.")
    if failed:
        print("Failed:", failed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Nifty 200 daily OHLCV from Dhan")
    parser.add_argument("--symbol", help="Single symbol (e.g. RELIANCE)")
    parser.add_argument("--years", type=int, default=5, help="Years of history (default 5)")
    parser.add_argument("--force", action="store_true", help="Re-fetch even if file exists")
    parser.add_argument("--nifty500", action="store_true",
                        help="Use the full NIFTY_500 universe (existing files skipped unless --force)")
    args = parser.parse_args()

    base_map = NIFTY_500 if args.nifty500 else NIFTY_200
    syms = {args.symbol: base_map[args.symbol]} if args.symbol else (
        NIFTY_500 if args.nifty500 else None)
    if args.symbol and args.symbol not in base_map:
        print(f"ERROR: {args.symbol} not in {'NIFTY_500' if args.nifty500 else 'NIFTY_200'} map in instruments.py")
        sys.exit(1)
    main(symbols=syms, years=args.years, force=args.force)
