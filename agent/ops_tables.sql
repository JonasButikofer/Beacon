-- Tables for the data quality job (resources/beacon_quality.job.yml).
-- run_checks.py also creates these on first run, so running this by hand is optional.

CREATE SCHEMA IF NOT EXISTS beacon.ops
COMMENT 'Operational tables: data quality results and agent incident reports.';

-- agent/run_checks.py — one row per check per run
CREATE TABLE IF NOT EXISTS beacon.ops.dq_results (
    run_id       STRING    COMMENT 'Databricks job run id ({{job.run_id}}) — groups one run''s checks',
    check_ts     TIMESTAMP COMMENT 'When the check ran',
    check_name   STRING    COMMENT 'Check id from agent/checks.py, e.g. markets_freshness',
    table_name   STRING    COMMENT 'Table (or job) the check covers',
    severity     STRING    COMMENT 'error | warn — only error fails the job',
    status       STRING    COMMENT 'pass | fail | error (the check itself crashed)',
    message      STRING    COMMENT 'Human-readable result',
    observed     STRING    COMMENT 'JSON of the measured values'
)
USING DELTA
COMMENT 'Rule-based data quality results for beacon.bronze. Append-only.';

-- agent/diagnose.py — one row per run that had a non-passing check
CREATE TABLE IF NOT EXISTS beacon.ops.dq_incidents (
    run_id         STRING        COMMENT 'Job run id, joins to dq_results.run_id',
    created_ts     TIMESTAMP     COMMENT 'When the diagnosis was written',
    model          STRING        COMMENT 'Model that wrote the diagnosis',
    failed_checks  ARRAY<STRING> COMMENT 'Checks that did not pass',
    report         STRING        COMMENT 'Markdown diagnosis from the agent',
    input_tokens   BIGINT        COMMENT 'Total input tokens across agent turns',
    output_tokens  BIGINT        COMMENT 'Total output tokens across agent turns'
)
USING DELTA
COMMENT 'LLM-written diagnoses of failed data quality checks. Append-only.';
