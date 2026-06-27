# CLAUDE.md — HistoricalDataStocks

> Independent NSE equity OHLCV data store for backtesting.
> No imports from any strategy project. Any project that needs price history
> points at this folder's Parquet files — nothing else required.

---

## What This Is

A self-contained data pipeline for Nifty 200 equity OHLCV history:

| Layer | Source | Lookback | Storage |
|---|---|---|---|
| **Daily** | Dhan `historical_daily_data` | 5 years in 1 API call | `data/daily/{SYMBOL}.parquet` |
| **5min** | Dhan `intraday_minute_data` | 5 years in 88-day chunks | `data/5min/{SYMBOL}.parquet` |
| **Any other TF** | Rolled from 5min | Same as 5min | Not stored — computed on demand |

Derived TFs (15min, 25min, 75min, 125min, or any N that is a multiple of 5) are
computed by `rollup.py` with session-based bin anchoring at 09:15 IST. No separate
fetch or storage needed. Volume is always **summed** across constituent bars.

---

## Project Layout

```
HistoricalDataStocks/
├── instruments.py    — NIFTY_200 dict: symbol -> Dhan security_id
├── fetch_daily.py    — Download 5yr daily OHLCV (1 call/symbol)
├── fetch_5min.py     — Download 5yr 5min OHLCV (88-day chunks, resume-safe)
├── rollup.py         — Resample 5min to any N-min TF; session-aligned to 09:15 IST
├── query.py          — Public API: daily(), intraday(), universe_daily(), to_csv()
├── pyproject.toml    — uv-managed dependencies
├── symbols/
│   └── nifty200.txt  — 200 NSE symbols, one per line
└── data/
    ├── daily/        — {SYMBOL}.parquet  (~200 files, ~50 MB)
    └── 5min/         — {SYMBOL}.parquet  (~200 files, ~2-3 GB)
```

---

## Initial Data Fetch

**One-time setup (run once, takes ~20 minutes total):**

```
# Step 1 — Daily bars for all 200 symbols (~1 minute)
uv run python fetch_daily.py

# Step 2 — 5min bars for all 200 symbols (~18 minutes)
uv run python fetch_5min.py
```

**Incremental refresh (run weekly or before a backtest):**

```
uv run python fetch_daily.py --force          # re-fetch all daily (1 min)
uv run python fetch_5min.py --refresh         # fetch only new 5min bars
```

**Single symbol:**

```
uv run python fetch_daily.py --symbol RELIANCE
uv run python fetch_5min.py --symbol RELIANCE
```

---

## API Rate Limits (confirmed live 2026-06-28)

| Limit | Value | Notes |
|---|---|---|
| Requests per second | 5 | DH-904 error on 6th rapid call |
| Reset window | 1 second | Instant recovery after 1s |
| Daily TF lookback | 5+ years in single call | No chunking needed |
| 5min TF lookback | 5+ years exist; 90 days per call | DH-905 if window > 90 days |
| Type | Rate-based (req/sec) | NOT quota-based; Monday trading unaffected |

Scripts sleep 0.25s between calls (4 req/sec — safely under limit).

---

## Using Data in a Backtest

### From any project folder — no imports needed

```python
import duckdb
import pandas as pd

BASE = "D:/Trading/HistoricalDataStocks"

# Daily bars for one symbol
df = pd.read_parquet(f"{BASE}/data/daily/RELIANCE.parquet")

# Full Nifty 200 daily — DuckDB single pass
df = duckdb.query(f"""
    SELECT symbol, date, open, high, low, close, volume
    FROM read_parquet('{BASE}/data/daily/*.parquet')
    WHERE date >= '2024-01-01'
    ORDER BY symbol, date
""").df()
```

### Using query.py (recommended — handles TF rollups)

```python
import sys
sys.path.insert(0, "D:/Trading/HistoricalDataStocks")
from query import daily, intraday, universe_daily, to_csv

# Daily bars
df = daily("RELIANCE", from_date="2023-01-01")

# 125min bars (rolled from 5min, volume summed, 09:15 IST anchored)
df = intraday("RELIANCE", minutes=125)

# 25min bars for a date range
df = intraday("RELIANCE", minutes=25, from_date="2025-01-01", to_date="2025-12-31")

# All 200 symbols — daily (DuckDB pass, returns flat DataFrame with 'symbol' column)
universe = universe_daily(from_date="2024-01-01")

# Export to CSV
to_csv(df, "reliance_125min.csv")
```

### Supported timeframes

| TF | minutes | Bars/day | Notes |
|---|---|---|---|
| 5min | 5 | 75 | Raw Dhan data |
| 15min | 15 | 25 | 3 × 5min |
| 25min | 25 | 15 | 5 × 5min |
| 75min | 75 | 5 | 15 × 5min |
| 125min | 125 | 3 | 25 × 5min; same as MCIC-125 in TradingWebSockets |
| Any N | N | varies | N must be multiple of 5 |

---

## Rollup Details

`rollup.py` uses **session-based binning**, not pandas resample with a fixed origin.

Why: `pandas.resample(origin=...)` drifts for intervals that don't divide 1440
evenly (e.g. 125min: 1440 % 125 = 115 ≠ 0). Each day is binned independently,
anchored to 09:15 IST. Bins: 09:15, 09:15+N, 09:15+2N, ...

Volume aggregation: `SUM` of all constituent 5min volumes. This is required for
any volume-based indicator (VWAP, VWMA, OBV, volume ratio). Confirmed: 5min
volume sum == 25min sum == 125min sum for the same session.

---

## Dhan Date Shift (Daily TF)

Dhan's daily API consistently returns OHLCV for trading date T under a
timestamp one day earlier (T-1). This is documented in
`TradingWebSockets/brokers/dhan.py` (fetch_daily_data docstring).

**Impact for backtesting:**
- Purely technical (indicator-based): **no impact** — all bars shift equally,
  relative analysis is correct
- Event-driven (earnings, dividends, index rebalancing by date): apply +1
  trading day correction before matching dates

The stored `date` column in `data/daily/*.parquet` carries the Dhan-returned
(shifted) date. No auto-correction is applied — be aware when doing date-event work.

---

## Authentication

Scripts read Dhan credentials from `D:/Trading/TradingWebSockets/.env`:
- `DHAN_CLIENT_ID`
- `DHAN_ACCESS_TOKEN`

Token is auto-renewed daily at 08:50 IST by Windows Task Scheduler
("DhanTokenCheck") via `D:/Trading/renew_token.py`. No separate `.env` needed
for this project — it reuses TW's token.

If you get `DH-901 Invalid_Authentication`, run:
```
uv run python D:/Trading/renew_token.py --force-new
```

---

## Adding Nifty 500

When Nifty 500 coverage is needed:
1. Download the Dhan instruments CSV (same as `NiftyOptionsBacktest/src/dhan_instruments.py` but for `NSE_EQ` segment)
2. Filter to the 500 symbols and add to `instruments.py` as `NIFTY_500: dict[str, str]`
3. Create `symbols/nifty500.txt`
4. Pass `NIFTY_500` to `fetch_daily.py` and `fetch_5min.py` instead of `NIFTY_200`
5. `data/daily/` and `data/5min/` are flat directories — they absorb Nifty 500 naturally

---

## How Other Projects Use This

**Pattern A — Read Parquet directly (no dependency):**
Any script anywhere can do `pd.read_parquet("D:/Trading/HistoricalDataStocks/data/daily/RELIANCE.parquet")`.
No imports, no coupling. DuckDB can glob `data/daily/*.parquet` for universe scans.

**Pattern B — Import query.py (one sys.path line):**
```python
sys.path.insert(0, "D:/Trading/HistoricalDataStocks")
from query import daily, intraday
```
This gives access to rollup TFs without duplicating the rollup logic.

**Pattern C — Run as subprocess from a notebook or script:**
```python
import subprocess
subprocess.run(["uv", "run", "--project", "D:/Trading/HistoricalDataStocks",
                "python", "fetch_daily.py", "--symbol", "RELIANCE"])
```

**Do NOT:**
- Import `instruments.py` or copy security IDs into other projects — they must come from here
- Store rolled-up TFs (15min, 25min, etc.) as separate Parquet files — derive them on demand
- Use this project's venv for strategy execution — keep it data-only

---

## Known Limitations

- **Nifty 500:** Not yet built. Only Nifty 200 (from `TradingWebSockets/config/settings.py` NIFTY_200_SECURITY_MAP).
- **Corporate actions:** No dividend/split adjustment. Raw Dhan prices only.
- **Survivorship bias:** Symbols reflect current Nifty 200 composition. Delisted or replaced stocks are not included.
- **5min weekends/holidays:** Dhan returns empty for non-trading days. Gaps are natural — no rows for those dates.

---

## Settled Decisions

**#1 — Store only 5min, derive everything else.**
No 15min/25min/75min/125min Parquets. Rollup on demand. Adding a new TF = zero fetch, zero storage.

**#2 — session-based binning, not pandas resample.**
pandas resample with fixed origin drifts for non-1440-factor intervals. Session grouping is correct and simple.

**#3 — Volume is always SUM in rollups.**
VWAP, VWMA, OBV, and all volume-ratio indicators require correct aggregated volume. Never average or last-tick volume.

**#4 — Auth from TW .env, not a separate .env.**
Token is managed by the existing auto-renewal infrastructure. No duplication.
