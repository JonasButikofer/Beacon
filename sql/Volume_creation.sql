-- Volumes for the Finnhub streaming pipeline.
-- quote_landing: JSON-lines files written by ingest/stream_finnhub_consumer.py.
-- checkpoints:   Auto Loader checkpoint dirs (e.g. checkpoints/quotes_raw) for ingest/stream_finnhub_autoloader.py.
CREATE VOLUME IF NOT EXISTS beacon.bronze.quote_landing;
CREATE VOLUME IF NOT EXISTS beacon.bronze.checkpoints;
