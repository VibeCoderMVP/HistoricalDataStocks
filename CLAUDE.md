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
| **Sector indices (5min)** | Dhan `intraday_minute_data`, `exchange_segment=IDX_I` | 5 years where available, most back to 2021-08 (see below) | `data/sector_5min/{SECTOR_NAME}.parquet` |
| **Sector indices (daily)** | Dhan `historical_daily_data`, `exchange_segment=IDX_I` | 2021-08-31 onward for most; a few from 2023-07 (see below) | `data/sector_daily/{SECTOR_NAME}.parquet` |

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
├── fetch_sector_daily.py — Download daily OHLC for NSE sector indices (IDX_I segment)
├── fetch_sector_membership.py — Refresh sector_membership.json from NSE's constituent CSVs
├── sector_membership.json — symbol -> [sector(s)] map (relocated here 2026-08-31)
├── rollup.py         — Resample 5min to any N-min TF; session-aligned to 09:15 IST
├── query.py          — Public API: daily(), intraday(), universe_daily(), to_csv()
├── pyproject.toml    — uv-managed dependencies
├── symbols/
│   └── nifty200.txt  — 200 NSE symbols, one per line
└── data/
    ├── daily/        — {SYMBOL}.parquet  (~200 files, ~50 MB)
    ├── sector_5min/  — {SECTOR_NAME}.parquet (13 files, e.g. "NIFTY IT.parquet")
    ├── sector_daily/ — {SECTOR_NAME}.parquet (13 files)
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

**The 5 req/sec limit is account-wide, not per-process (confirmed live 2026-08-19).**
`fetch_daily.py`'s 4 req/sec budget only accounts for itself — it does not know about
(or coordinate with) any other Dhan-calling process sharing the same
`DHAN_ACCESS_TOKEN`. Running `INGEST_DAILY_BARS.bat`/`fetch_daily.py --force` while any
live fleet poller is also active on Dhan (most commonly `TradingCommodities`' P1C, which
polls MCX every ~4.5min through its own 23:55 session end — see
`trading_core/CLAUDE.md`'s "Daily Bars Store" section for the ingestion pipeline this
feeds) can push the combined call rate over 5/sec and produce real `DH-904 Rate_Limit`
rejections on this script's own calls, even though `fetch_daily.py` itself never
exceeds 4/sec. This is timing-dependent, not deterministic — a run can succeed cleanly
one day and hit rejections the next with no code or config change, purely because the
1-minute fetch window happened to overlap another poller's own burst. See
`bug_fetch_daily_dh904_rate_limit_contention` in project memory for the incident that
surfaced this.

**Fix (2026-08-19): `fetch_one()` now retries up to 3 times (1s backoff) on any
non-success response, and logs Dhan's actual `remarks`/`data` on each failed attempt
and on final give-up** — previously a rejection (rate-limit or otherwise) silently
became `None` → `— FAILED` with zero diagnostic info. This self-heals the common
rate-limit-contention case (confirmed live: every retried symbol from a real
90-failure run succeeded on attempt 2) without needing a cross-process rate
coordinator. A symbol that still fails after 3 attempts is a genuine problem worth a
closer look — re-run `fetch_daily.py --symbol SYMBOL --force` standalone once
contention has cleared, or check the printed error for a different root cause (bad
security_id, auth, etc.).

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
# NOTE: MOTHERSON has known-corrupted daily data (duplicate rows, see Known
# Limitations below) — excluded here explicitly since this glob bypasses
# query.py's automatic exclusion.
df = duckdb.query(f"""
    SELECT symbol, date, open, high, low, close, volume
    FROM read_parquet('{BASE}/data/daily/*.parquet')
    WHERE date >= '2024-01-01' AND symbol != 'MOTHERSON'
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

## Nifty 500

**Daily TF: DONE (2026-09-08).** `instruments.py::NIFTY_500` (500 symbols, superset of
`NIFTY_200` with identical ids) built from NSE `ind_nifty500list.csv`
(`symbols/ind_nifty500list.csv`) matched against `trading_core/data/dhan_instruments_master.csv`
(NSE / EQUITY / EQ|BE series). `symbols/nifty500.txt` written. `DUMMYHEG` (an NSE placeholder
scrip) dropped → 500 real names, 300 new vs Nifty200.

- Fetch: `uv run python fetch_daily.py --nifty500` (existing 200 files skipped unless `--force`).
  All 300 new fetched clean, 0 failures; 222 have full ~5yr history, 78 are recent IPOs with
  shorter history (expected).
- **Split-adjustment verified**: Dhan's `historical_daily_data` back-adjusts the mid-cap daily
  series exactly as it does large-caps — 64 split/bonus events across 55 of the new 300,
  cross-checked against NSE's own corporate-actions feed (`data/corporate_actions_raw.parquet`),
  every one shows a normal price transition across the ex-date, none shows the split ratio.
  A pure daily-TF backtest on the full Nifty500 is safe.
- Downstream: `trading_core.daily_bars.backfill_from_hds()` globs all `data/daily/*.parquet`, so
  `python -m trading_core.daily_bars backfill-hds <dir>` picks up all 500 with no code change —
  done, DailyBarStore now has full 5yr for the new 300.
- Tier tagging: `HurdleCheck/nifty500_symbols.csv` + `load_tiers()` adds an `SMCS` tier
  (Nifty500 − Nifty200, ~300); `PnF_Charts/config/universes.json` got a `nifty_500` set.

**5min TF: NOT done** — deferred by the user 2026-09-08. The blocker
(`bug_hds_5min_daily_split_adjustment_mismatch`) is fixed on read since 2026-10-26 (see
"Intraday Corporate-Action Adjustment"); a Nifty500 5min fetch would also need
`build_intraday_adjustment.py` re-run over the new symbols. `fetch_5min.py` has no
`--nifty500` flag yet.

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
  niftynext50.txt`. **Relocated here 2026-08-31** (was
  `D:\Trading\backtesting\GapDC-Test\symbols\sector_membership.json`,
  project-local — moved once a second consumer, the daily/5min sector bar
  store work, needed the same mapping, per the original file's own "move it
  here if a second project needs it" comment). New
  `fetch_sector_membership.py` (this repo) rebuilds `sector_membership.json`
  (also this repo, sibling to `instruments.py`) from NSE's live CSVs and
  reports drift against the current `symbols/nifty200.txt` universe in the
  same run (`--check-only` to just report, not write) — `GapDC-Test/
  sector_correlation.py` now reads this repo's copy directly, not its own.
  As of the 2026-08-31 refresh: covers 115 of the 200 universe symbols; 85 are
  uncovered (industrials, telecom, power, cement, AMCs, new listings — not
  part of any of these 13 sectors) and 48 rows are stale (mapped symbols no
  longer in the current universe, e.g. delisted/renamed/replaced) — same
  counts as the original 2026-07-12 fetch, confirming NSE's own constituent
  lists genuinely haven't been revised since then (their own reconstitution
  reviews are periodic, not continuous — re-run this script periodically,
  it won't self-trigger). **A same-day refresh does not make a brand-new IPO
  sector-correct instantly** — e.g. HYUNDAI (an auto company) still shows as
  uncovered as of this refresh, because NSE's own NIFTY AUTO constituent list
  predates its listing; that only resolves at NSE's next reconstitution
  review, not by refreshing this script sooner.
  **Filename gotchas** (NSE's URLs aren't perfectly systematic): financial
  services is `ind_niftyfinancelist.csv`, NOT `ind_niftyfinancialserviceslist.csv`;
  `ind_niftyprivatebanklist.csv` does not exist as a standalone file (private
  banks are folded into `ind_niftybanklist.csv`). `fetch_sector_membership.py`
  tries a candidate-slug list per sector and verifies each response actually
  has a `Symbol` column before trusting it — never guesses blind.
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

**Daily added 2026-08-31** (`fetch_sector_daily.py`, sibling to `fetch_index_daily.py` but
reads `SECTOR_INDICES` instead of `BROAD_INDICES`) → `data/sector_daily/{SECTOR_NAME}.parquet`,
one `historical_daily_data()` call per sector, no chunking needed (unlike the 5min endpoint's
90-day window limit). **Daily history goes back further than 5min for some sectors** — e.g.
NIFTY CONSUMER DURABLES and NIFTY OIL AND GAS have full 2021-08-31 daily history despite only
having 5min data from 2024 onward; Dhan's daily endpoint isn't bound by the same per-index
history-start limit as the 5min intraday one. Live-side counterpart:
`trading_core.sector_daily_bars.SectorDailyBarStore` (`data/sector_daily_bars.db`, backfill-only,
no live poller — see `trading_core/CLAUDE.md`'s own entry).

**5min store found stale 2026-08-31** (last refreshed at its original 2026-07-12 build) — the
same session's sector-data audit found `TradingWebSockets/rest_candle_service.py`'s live sector
poll paused (`POLL_SECTORS=0` since PR-04, no live consumer) and this offline parquet never
re-run since. Caught up via `fetch_sector_5min.py --refresh`, now current through today; also
found the real per-index 5min history-start is 2021-08-04 for most sectors, not just ~5yr as
originally documented.

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

## Intraday Corporate-Action Adjustment (added 2026-10-26)

Fix for `bug_hds_5min_daily_split_adjustment_mismatch` (SEV1). The intraday parquet is left
untouched (raw ingest target); a per-session **factor table** puts it on the daily store's
adjusted scale, applied on read:

```python
sys.path.insert(0, "D:/Trading/HistoricalDataStocks")
from query import intraday
df = intraday("360ONE", minutes=125, adjusted=True)          # via query.py
from intraday_adjust import apply_intraday_adjustment          # or on any frame you loaded
df = apply_intraday_adjustment(df, symbol="360ONE", tf="5min")  # tf="1min" for 1min data
```

- **Why reconcile to daily, not apply NSE ratios:** the "raw" intraday is not consistently raw —
  Dhan returned some events already adjusted at fetch time (RELIANCE 2024 bonus, POWERGRID 2021
  bonus) and others not (360ONE, IOC, SRF...). `factor = daily_high / intraday_high` per session
  (+1-day daily date fix), constant within a corporate-action segment (e.g. 360ONE 0.25000,
  BSE 0.33333, IOC 0.66667, ITC demerger 0.94600). Boundaries land on the NSE ex-date.
- Sessions before the daily store's first date (2021-06-28..10-26) use NSE ratios from
  `confirmed_splits.json`, only where the stored bars show the unadjusted jump (SRF, APLAPOLLO).
- **Rebuild after every 5min/1min refresh that crosses a new corporate action:**
  `.venv/Scripts/python.exe build_intraday_adjustment.py --tf 5min` (and `--tf 1min`), then
  `validate_intraday_adjustment.py --tf 5min`. Output: `data/intraday_adjustment_<tf>.parquet`
  (+ `_segments.csv` audit, `_validation.csv`). Sessions newer than the table get factor 1.0.
- Validated 2026-10-26: daily/intraday ratio spread >1.3x went 8 symbols -> 1 (MOTHERSON); 71/78
  NSE events clean after adjustment; 360ONE's remaining +10.47% bonus-day gap equals Kite's
  independently adjusted gap (a real move). 125min rollup-then-adjust == precomputed-then-adjust.
- **Still excluded:** MOTHERSON (corrupt daily reference -> not in the table). NMDC 2022-10-27
  and GMRAIRPORT 2022-01-11 demergers are unadjusted in the *daily* store too, so both stores
  agree but the price series carries the demerger gap.
- Volume is not adjusted (daily volume isn't either). Default `adjusted=False` keeps existing
  callers unchanged — consumers must opt in and can then drop the 11+5 symbol exclusions
  (except MOTHERSON).

---

## Known Limitations

- **Nifty 500:** Daily TF built 2026-09-08 (`instruments.py::NIFTY_500`, `data/daily/` now 500
  files, split-adjustment verified — see "Nifty 500" section above). 5min/1min TF still Nifty 200
  only (deferred by the user; the intraday split-adjustment blocker is fixed on read since 2026-10-26).
- **Corporate actions:** the **daily** store IS split/bonus-adjusted throughout its history
  (confirmed 2026-07-26 against NSE's real corporate-action ex-dates via
  `fetch_corporate_actions.py` — no price discontinuity at any confirmed split/bonus date).
  The **intraday** stores (5min and derived 15/25/75/125min) are NOT adjusted before a
  corporate action — see `bug_hds_5min_daily_split_adjustment_mismatch` in project memory.
  Any backtest that mixes a daily/longer-TF structural signal with an intraday execution
  clock is exposed to this; a pure daily-TF backtest is not.
  **Fixed on read since 2026-10-26** — see "Intraday Corporate-Action Adjustment" above.
- **MOTHERSON daily data is corrupted — excluded by default, do not re-add:** 704 duplicate-date
  rows (57% of its history) at two conflicting price scales ~1.35x apart. Not worth fixing
  (one symbol out of 200 doesn't materially change a universe-level backtest decision).
  `query.py`'s `universe_daily()` excludes it automatically (`KNOWN_BAD_DAILY_SYMBOLS`); `daily()`
  still allows direct access with a warning. **If reading `data/daily/*.parquet` directly**
  (Pattern A below, bypassing `query.py`) **you must filter it out yourself** — the exclusion
  only exists in `query.py`. See `bug_hds_mothersonso_daily_duplicate_rows` in project memory.
- **Survivorship bias:** Symbols reflect current Nifty 200 composition. Delisted or replaced stocks are not included.
- **5min weekends/holidays:** Dhan returns empty for non-trading days. Gaps are natural — no rows for those dates.
- **`data/daily/*.parquet` is a true rolling 5-year window, not an accumulating archive
  (clarified 2026-07-27).** `fetch_daily.py --force` calls `df.to_parquet(out, index=False)` —
  a full overwrite of each symbol's file with only the freshly-fetched `today - 5yrs` to `today`
  range, no merge with whatever was there before. Anything older than 5 years back silently
  drops out of this store as time passes. This is fine for HDS's own direct consumers (5yr is
  the documented lookback everywhere), but used to matter for `trading_core.daily_bars.
  DailyBarStore` (`daily_bars.duckdb`), which treats this parquet as an authoritative daily-TF
  overlay run fresh every day (see `trading_core/CLAUDE.md`'s "Daily Bars Store" section) —
  that consumer previously only ever upserted, never deleted, so its own history kept
  accumulating past 5 years even as this store's own rolling window moved forward. **This was
  wrong, not benign** (fixed 2026-08-20, see `bug_daily_bars_rolling_window_split_boundary`,
  memory): a bar that ages out of THIS store's window is never re-fetched/re-adjusted again, so
  if a symbol's split ex-date falls after one of its old bars has already aged out of this
  parquet, `daily_bars.duckdb`'s copy of that old bar was frozen on the pre-split scale
  permanently, with no code path left to fix it — confirmed live (6,898 already-orphaned rows
  across 182 symbols existed at fix time). `trading_core.daily_bars.backfill_from_hds()` now
  purges any of its own rows older than this store's current window floor on every run, so
  `daily_bars.duckdb` stays correctly bounded to this rolling ~5yr window going forward — any
  backtest wanting deeper daily history than that needs its own separately-sourced dataset, not
  an assumption that the shared store still has it.

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
