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
| **5min** | Dhan `intraday_minute_data` (interval=5) | 5 years in 88-day chunks | `data/5min/{SYMBOL}.parquet` |
| **1min** | Dhan `intraday_minute_data` (interval=1) | 5 years in 88-day chunks (confirmed live 2026-07-11) | `data/1min/{SYMBOL}.parquet` |
| **Any other TF** | Rolled from 5min | Same as 5min | Not stored — computed on demand |
| **Sector indices (5min)** | Dhan `intraday_minute_data`, `exchange_segment=IDX_I` | 5 years where available (see below) | `data/sector_5min/{SECTOR_NAME}.parquet` |

Derived TFs (15min, 25min, 75min, 125min, or any N that is a multiple of 5) are
computed by `rollup.py` with session-based bin anchoring at 09:15 IST. No separate
fetch or storage needed. Volume is always **summed** across constituent bars.

---

## Project Layout

```
HistoricalDataStocks/
├── instruments.py    — NIFTY_200 dict: symbol -> Dhan security_id
│                        + SECTOR_INDICES dict: sector name -> Dhan security_id (IDX_I)
├── fetch_daily.py    — Download 5yr daily OHLCV (1 call/symbol)
├── fetch_5min.py     — Download 5yr 5min OHLCV (88-day chunks, resume-safe)
├── fetch_1min.py     — Download 5yr 1min OHLCV (88-day chunks, resume-safe)
├── fetch_sector_5min.py — Download 5yr 5min OHLCV for NSE sector indices (IDX_I segment)
├── rollup.py         — Resample 5min to any N-min TF; session-aligned to 09:15 IST
├── query.py          — Public API: daily(), intraday(), universe_daily(), to_csv()
├── pyproject.toml    — uv-managed dependencies
├── symbols/
│   └── nifty200.txt  — 200 NSE symbols, one per line
└── data/
    ├── daily/        — {SYMBOL}.parquet  (~200 files, ~50 MB)
    ├── sector_5min/  — {SECTOR_NAME}.parquet (13 files, e.g. "NIFTY IT.parquet")
    ├── 5min/         — {SYMBOL}.parquet  (~200 files, ~2-3 GB)
    └── 1min/         — {SYMBOL}.parquet  (~200 files, ~1.5-2 GB est.)
```

---

## Initial Data Fetch

**One-time setup (run once, takes ~20 minutes total):**

```
# Step 1 — Daily bars for all 200 symbols (~1 minute)
uv run python fetch_daily.py

# Step 2 — 5min bars for all 200 symbols (~18 minutes)
uv run python fetch_5min.py

# Step 3 (optional) — 1min bars for all 200 symbols (~18 min in API calls;
# ~5x the row count of 5min, so allow more time for parquet writes)
uv run python fetch_1min.py
```

**Incremental refresh (run weekly or before a backtest):**

```
uv run python fetch_daily.py --force          # re-fetch all daily (1 min)
uv run python fetch_5min.py --refresh         # fetch only new 5min bars
uv run python fetch_1min.py --refresh         # fetch only new 1min bars
```

**Single symbol:**

```
uv run python fetch_daily.py --symbol RELIANCE
uv run python fetch_5min.py --symbol RELIANCE
uv run python fetch_1min.py --symbol RELIANCE
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

## Sector Indices (added 2026-07-12, GapDC-Test sector-correlation build)

Dhan's instrument master (`https://images.dhan.co/api-data/api-scrip-master.csv`)
lists every NSE sector index (NIFTY IT, NIFTY BANK, NIFTY METAL, NIFTY AUTO,
NIFTY PHARMA, NIFTY FMCG, NIFTY REALTY, NIFTY MEDIA, NIFTY PSU BANK, NIFTY
HEALTHCARE, NIFTY CONSUMER DURABLES, NIFTY OIL AND GAS, plus a NIFTY FINANCIAL
SERVICES proxy — see caveat below) under `SEM_EXCH_INSTRUMENT_TYPE=INDEX`,
`SEM_EXM_EXCH_ID=NSE`. This was previously assumed to be unavailable/unresearched
— it is available, and fetchable through the exact same
`dhanhq.intraday_minute_data()` call already used for equities, just with
`exchange_segment="IDX_I"` / `instrument_type="INDEX"` (`dhanhq.dhanhq.INDEX =
'IDX_I'`, a real package constant) instead of `NSE_EQ`/`EQUITY`.

- **security_ids**: `instruments.py`'s `SECTOR_INDICES` dict, 13 sectors.
- **OHLC fetch**: `fetch_sector_5min.py` (sibling to `fetch_5min.py`, same
  chunking/resume-safe logic) → `data/sector_5min/{SECTOR_NAME}.parquet`.
  4 of the 13 sectors (NIFTY FINANCIAL SERVICES proxy, NIFTY HEALTHCARE, NIFTY
  CONSUMER DURABLES, NIFTY OIL AND GAS) only have Dhan history from 2024 onward
  — these are newer index variants, not a fetch failure; trades/analysis before
  each sector's own data-start date should be treated as unknown for that
  sector, not backfilled or assumed flat.
- **Constituent membership** (which equities belong to which sector — separate
  from the OHLC above) comes from NSE's public archive, NOT from Dhan:
  `https://nsearchives.nseindia.com/content/indices/ind_<name>list.csv`, same
  fetch pattern already used once in this repo for `DC-Separate/symbols/
  niftynext50.txt`. The parsed symbol→sector(s) map lives in
  `D:\Trading\backtesting\GapDC-Test\symbols\sector_membership.json` (project-
  local, not here, since only that project has consumed it so far — move it
  here if a second project needs the same mapping). Covers 115 of this
  project's 200 universe symbols; the other 85 (industrials, telecom, power,
  cement, AMCs, new listings) aren't part of any of these 13 sectors.
  **Filename gotchas** (NSE's URLs aren't perfectly systematic): financial
  services is `ind_niftyfinancelist.csv`, NOT `ind_niftyfinancialserviceslist.csv`;
  `ind_niftyprivatebanklist.csv` does not exist as a standalone file (private
  banks are folded into `ind_niftybanklist.csv`). Verify with an HTTP status
  check before assuming a guessed filename works.
- **Caveat — "NIFTY FINANCIAL SERVICES" name mismatch**: Dhan does not expose
  the classic full Financial Services index under that name in `IDX_I`. `NIFTY
  FINSRV25 50` (security_id 469, a newer 25/50-stock derivatives-linked variant)
  is used as the closest available OHLC proxy in `SECTOR_INDICES`, while
  membership still comes from NSE's classic `ind_niftyfinancelist.csv`
  constituent list — the OHLC series and the membership list are not from
  perfectly matching index definitions for this one sector. Re-verify before
  leaning on this sector specifically for anything precision-sensitive.
- **What this does NOT cover**: no pre-market/pre-open (09:00-09:15 IST call
  auction) index data has been investigated or fetched — `intraday_minute_data`
  history above starts from the 09:15 regular session same as equities. No live
  tick subscription for sector indices exists in TradingWebSockets' P1/P2 either
  — this is historical-only, offline-backtest infrastructure so far. See
  `reference_dhan_sector_index_data` and `reference_dhan_premarket_data` in
  project memory for the live/pre-market research thread if picking this up.

**Extending coverage**: the same instrument-master CSV has ~100 more niche/
strategy indices (NIFTY INFRA, NIFTYPSE, NIFTY COMMODITIES, etc.) that could
close some of the 85-symbol gap above — re-run the same
filter-by-`SEM_EXCH_INSTRUMENT_TYPE=INDEX` approach, then check NSE for a
matching constituent CSV before assuming one exists.

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

**#1 — Store only 5min, derive everything else (superseded 2026-07-01 for hot TFs).**
Original rule: no 15/25/75/125min Parquets, rollup on demand. In practice, repeated
on-demand rollup across 200 symbols is slow enough that hot TFs get precomputed and
cached: `data/125min/` (built by `precompute_125min.py`) and, as of 2026-07-01,
`data/15min/`, `data/25min/`, `data/75min/` (built by `precompute_tf.py --minutes N`,
the generalized version). Same schema as 125min: `datetime, open, high, low, close,
volume, symbol`. Re-run `precompute_tf.py --minutes N --refresh` after a 5min data
refresh to keep these caches current. Any TF not in this list is still derived on
demand via `rollup.py` — no need to precompute everything.

**#2 — session-based binning, not pandas resample.**
pandas resample with fixed origin drifts for non-1440-factor intervals. Session grouping is correct and simple.

**#3 — Volume is always SUM in rollups.**
VWAP, VWMA, OBV, and all volume-ratio indicators require correct aggregated volume. Never average or last-tick volume.

**#4 — Auth from TW .env, not a separate .env.**
Token is managed by the existing auto-renewal infrastructure. No duplication.
