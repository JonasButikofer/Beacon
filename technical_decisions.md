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
