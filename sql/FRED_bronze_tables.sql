-- Bronze table DDL for the FRED ingestion job (ingest/batch_fred.py).
-- macro_raw already exists (created via inferred schema on first PySpark write,
-- so it has no table/column comments yet); this documents its actual schema.

-- ingest/batch_fred.py — macro observations for the tracked FRED series, incremental MERGE
CREATE TABLE IF NOT EXISTS beacon.bronze.macro_raw (
    series_id     STRING    COMMENT 'FRED series ID, e.g. CPIAUCSL — joins to gold.dim_indicator',
    obs_date      DATE      COMMENT 'Observation date',
    value         DOUBLE    COMMENT 'Observed value (FRED "." missing markers are dropped)',
    _ingested_at  TIMESTAMP COMMENT 'When this row was loaded'
)
USING DELTA
COMMENT 'Macro observations from FRED /fred/series/observations for the tracked series. Incremental MERGE keyed on (series_id, obs_date).';
