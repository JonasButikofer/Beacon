-- Bronze table DDL for the Tiingo ingestion jobs (ingest/batch_tiingo_*.py).
-- All four tables exist. markets_raw was created via inferred schema on first
-- PySpark write, so it has no comments in the workspace. tickers_raw and
-- tickers_universe are rewritten each run with overwrite + overwriteSchema, which
-- drops the column comments below (the table comment survives).

-- ingest/batch_tiingo_eod.py — daily OHLCV bars for the watchlist, incremental MERGE
CREATE TABLE IF NOT EXISTS beacon.bronze.markets_raw (
    symbol        STRING    COMMENT 'Ticker symbol, e.g. SPY',
    obs_date      DATE      COMMENT 'Trading date',
    open          DOUBLE    COMMENT 'Raw open price',
    high          DOUBLE    COMMENT 'Raw high price',
    low           DOUBLE    COMMENT 'Raw low price',
    close         DOUBLE    COMMENT 'Raw close price',
    volume        BIGINT    COMMENT 'Raw share volume',
    adj_open      DOUBLE    COMMENT 'Split/dividend-adjusted open',
    adj_high      DOUBLE    COMMENT 'Split/dividend-adjusted high',
    adj_low       DOUBLE    COMMENT 'Split/dividend-adjusted low',
    adj_close     DOUBLE    COMMENT 'Split/dividend-adjusted close',
    adj_volume    BIGINT    COMMENT 'Split/dividend-adjusted volume',
    div_cash      DOUBLE    COMMENT 'Cash dividend paid on this date, if any',
    split_factor  DOUBLE    COMMENT 'Split ratio applied on this date (1.0 = no split)',
    _ingested_at  TIMESTAMP COMMENT 'When this row was loaded'
)
USING DELTA
COMMENT 'Daily OHLCV bars from Tiingo /tiingo/daily/<symbol>/prices, watchlist ETFs only. Incremental MERGE keyed on (symbol, obs_date).';

-- ingest/batch_tiingo_tickers.py — rich per-symbol metadata for the watchlist, full refresh
CREATE TABLE IF NOT EXISTS beacon.bronze.tickers_raw (
    symbol         STRING    COMMENT 'Ticker symbol',
    name           STRING    COMMENT 'Company / fund name',
    asset_type     STRING    COMMENT 'ETF | Stock, as tagged in the ingestion job watchlist',
    exchange_code  STRING    COMMENT 'Listing exchange',
    start_date     DATE      COMMENT 'First date Tiingo has price history for this symbol',
    end_date       DATE      COMMENT 'Last date Tiingo has price history for this symbol (null if still active)',
    description    STRING    COMMENT 'Tiingo-provided description',
    _ingested_at   TIMESTAMP COMMENT 'When this row was loaded'
)
USING DELTA
COMMENT 'Per-symbol metadata from Tiingo /tiingo/daily/<symbol>, watchlist only. Full overwrite each run.';

-- ingest/batch_tiingo_universe.py — full Tiingo-supported symbol list, thin columns
CREATE TABLE IF NOT EXISTS beacon.bronze.tickers_universe (
    ticker          STRING    COMMENT 'Ticker symbol',
    exchange        STRING    COMMENT 'Listing exchange',
    asset_type      STRING    COMMENT 'Stock | ETF | Mutual Fund | etc., as classified by Tiingo',
    price_currency  STRING    COMMENT 'Currency prices are quoted in',
    start_date      DATE      COMMENT 'First date Tiingo has price history for this symbol',
    end_date        DATE      COMMENT 'Last date Tiingo has price history for this symbol',
    _ingested_at    TIMESTAMP COMMENT 'When this row was loaded'
)
USING DELTA
COMMENT 'Full Tiingo-supported symbol universe (~100k+ tickers) from the bulk supported_tickers.zip. No name/description — join tickers_raw for the watchlist''s richer metadata. Full overwrite each run.';

-- ingest/batch_tiingo_news.py — news articles tagged to the watchlist, incremental MERGE
CREATE TABLE IF NOT EXISTS beacon.bronze.news_raw (
    article_id      STRING          COMMENT 'Tiingo article id',
    title           STRING          COMMENT 'Article title',
    description     STRING          COMMENT 'Article summary/description',
    url             STRING          COMMENT 'Source URL',
    source          STRING          COMMENT 'Publisher name',
    published_date  TIMESTAMP       COMMENT 'When the article was published',
    crawl_date      TIMESTAMP       COMMENT 'When Tiingo ingested the article',
    tickers         ARRAY<STRING>   COMMENT 'Tickers this article is tagged with',
    tags            ARRAY<STRING>   COMMENT 'Tiingo topic tags',
    _ingested_at    TIMESTAMP       COMMENT 'When this row was loaded'
)
USING DELTA
COMMENT 'News articles from Tiingo /tiingo/news for the watchlist. Free tier has no historical archive — only forward-looking from first run. Incremental MERGE keyed on article_id.';
