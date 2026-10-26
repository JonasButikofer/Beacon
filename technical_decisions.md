# Technical Decisions

> Quick notes, not the final write-up.

## Notes for later

- **Tiingo adjusted prices:** Tiingo rewrites `adj_*` for all history after a split/dividend. `batch_tiingo_eod.py` re-pulls a symbol's full history when a new row has `div_cash > 0` or `split_factor != 1`; MERGE updates only rows whose adjusted values changed. ~1 full re-pull per symbol per quarter (ETF dividends).
- **FRED revisions:** each run re-pulls 400 days behind the watermark; MERGE overwrites changed values. Bronze = latest vintage, not first print. Annual benchmark revisions older than 400 days are missed. First-print "surprise" analysis would need ALFRED vintages.
- **Tiingo news `startDate`:** API takes `YYYY-MM-DD` only, so the watermark is truncated to a date; same-day repeats are deduped by the MERGE on `article_id`.
- **`dim_indicator` moved to a dbt seed** (`dbt/seeds/dim_indicator.csv`) so dbt owns everything in `beacon.gold`. Replaced the hand-run `sql/FRED_data_description_Gold.sql`. Custom `generate_schema_name` macro so `+schema: gold` lands in `gold`, not `silver_gold`.

## Also worth writing up

- Finnhub consumer runs market hours only (Free Edition slot limits) instead of a continuous trigger.
- Auto Loader uses `availableNow` every 30 min + a final sweep, instead of `processingTime`.
- Batch ingest tasks run in parallel (separate tables) instead of chained.
- Jobs defined as a Databricks bundle (`resources/*.yml`) instead of exported job JSON.
- Ticker universe from the bulk zip, weekly, instead of per-symbol calls.

## dbt (Silver / Gold)

- **`stg_markets` is a full rebuild, not incremental.** Tiingo rewrites `adj_*` history after every dividend, so an incremental model would keep stale adjusted prices. It's ~9 symbols × ~9k days, so a rebuild is cheap.
- **`int_macro_daily` computes changes per observation, then forward-fills.** Computing YoY on the daily grid would compare today's value (last month's print) with the value from exactly a year ago (that month's print), giving an 11-month change.
- **Macro values are as of `obs_date`, not release date.** September CPI is dated 09-01 but published mid-October, so marts see it ~6 weeks early. That's fine for describing regimes; for backtesting signals against `fwd_21d_return` it's look-ahead bias. The fix would be a per-series release lag in `dim_indicator`.
- **`stg_trades` dedups on a hash of every field.** Finnhub has no trade id, so two genuinely identical prints in the same millisecond collapse into one.
- **`mart_macro_market_signals` is wide (one column per indicator), not long.** That suits dashboards and per-row regime logic; `int_macro_daily` is the long version.
- **dbt runs in `beacon_daily` with `run_if: ALL_DONE`.** One failed ingest task shouldn't stop the other sources' Silver/Gold from refreshing; the checks flag the failure. Source freshness runs after `dbt build` so a stale source reports without blocking.
- **Local dbt is Fusion (2.x); the job's `dbt_task` is dbt-core (1.10+).** The project sticks to syntax both accept (`arguments:` on generic tests, `config:` for source freshness).
