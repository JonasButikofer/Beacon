# Beacon review checklist

Read by the Claude code review workflows (`.github/workflows/claude-review-*.yml`).
Ordered by how much damage a miss does. Edit freely; no code change needed.

## Data correctness and idempotency
- Every bronze write is a `MERGE` on the table's natural key (`(symbol, obs_date)`, `(series_id, obs_date)`, `article_id`) or a deliberate full overwrite. Running a job twice must not create duplicates.
- Watermark logic: check off-by-one at the boundary, date truncation (Tiingo takes `YYYY-MM-DD`), the FRED revision lookback (400 days), and full re-pull after a split or dividend (`ingest/batch_tiingo_eod.py`).
- Schema changes to a bronze table are matched in the `sql/*.sql` DDL.

## Secrets and safety
- API keys come only from `dbutils.secrets.get("beacon", ...)`. Never in code, YAML, logs, or exception text written to tables (this leaked a key once, in `agent/diagnose.py`).
- `.env` files are never committed.
- The data quality agent's tools in `agent/diagnose.py` stay read-only: don't loosen the SQL allowlist, `READABLE_DIRS`, or `scrub()` without a stated reason.

## Reliability
- Every outbound HTTP call has a `timeout` and `raise_for_status()` or an explicit status check.
- "No data" from an API is distinguished from "not allowed" (403) and from "first run" (empty table).

## Databricks jobs and bundles (`resources/*.yml`, `databricks.yml`)
- Peak concurrent tasks stay at 5 or fewer (Free Edition limit). A `run_job_task` holds a slot while its child job runs.
- No overlapping schedules on the same Auto Loader checkpoint.
- Schedules go on `beacon_daily`, not on the child jobs it already starts (`beacon_batch`, `beacon_quality`).
- `run_if` is right: data quality must run even when ingest fails (`ALL_DONE`).
- Don't rely on `max_retries: 0`; serverless doesn't keep it.

## Data quality agent (`agent/`)
- New checks use `@check`, return `Result`, choose a severity deliberately, and read thresholds from `config.py`, with a comment explaining the number.
- A check that can't run yet (disabled source, new job) returns `[]` or passes instead of failing.

## Docs
- Changes to a dataset, job, or schedule update the `README.md` data inventory and, if relevant, `technical_decisions.md` or `agent/README.md`.

## What not to report
- Style or formatting issues.
- Anything without a concrete failure scenario (specific inputs or state leading to a wrong result).
- Lines the change didn't touch, unless the change breaks them.
