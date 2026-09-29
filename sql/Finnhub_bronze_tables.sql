-- Bronze table DDL for the Finnhub streaming pipeline
-- (ingest/stream_finnhub_consumer.py -> landing volume -> ingest/stream_finnhub_autoloader.py).
-- Created in the DEFAULT workspace; kept here as the reference definition.

CREATE TABLE IF NOT EXISTS beacon.bronze.quotes_raw (
    symbol        STRING    COMMENT 'Ticker symbol',
    price         DOUBLE    COMMENT 'Last trade price',
    volume        DOUBLE    COMMENT 'Trade volume',
    trade_time    BIGINT    COMMENT 'Trade timestamp, epoch milliseconds (raw from Finnhub)',
    conditions    ARRAY<STRING> COMMENT 'Finnhub trade condition codes',
    trade_ts      TIMESTAMP COMMENT 'trade_time cast to a proper timestamp',
    _ingested_at  TIMESTAMP COMMENT 'When this row was loaded by the Auto Loader job'
)
USING DELTA
COMMENT 'Live trade quotes streamed from Finnhub via WebSocket consumer -> landing volume -> Auto Loader. Append-only, watermarked by checkpoint, no dedup/MERGE.';
