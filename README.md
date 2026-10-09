# Beacon

> README is a work in progress. Only the data inventory is filled in so far.

## Data Sources

Everything lands in the `beacon.bronze` schema in Unity Catalog. API keys come from the `beacon` Databricks secret scope.

### Watchlist

The market-data jobs track the same 9 ETFs:

| Symbol | Coverage |
|---|---|
| SPY | S&P 500 |
| QQQ | Nasdaq 100 |
| DIA | Dow Jones Industrial Average |
| IWM | Russell 2000 (small-cap) |
| XLF | Financials sector |
| XLE | Energy sector |
| XLK | Technology sector |
| XLU | Utilities sector |
| TLT | 20+ Year Treasury bond |

### Ingested datasets

| Dataset | Source | Endpoint | Bronze table | Job | Load pattern |
|---|---|---|---|---|---|
| Macro indicators | FRED (St. Louis Fed) | `/fred/series/observations` | `macro_raw` | [batch_fred.py](ingest/batch_fred.py) | Batch, MERGE on `(series_id, obs_date)`; re-pulls the last 400 days each run so FRED revisions overwrite old values |
| Daily OHLCV prices | Tiingo | `/tiingo/daily/<symbol>/prices` | `markets_raw` | [batch_tiingo_eod.py](ingest/batch_tiingo_eod.py) | Batch, incremental MERGE on `(symbol, obs_date)`, backfills from 1990-01-01; a split or dividend triggers a full re-pull of that symbol to refresh `adj_*` history |
| Watchlist ticker metadata | Tiingo | `/tiingo/daily/<symbol>` | `tickers_raw` | [batch_tiingo_tickers.py](ingest/batch_tiingo_tickers.py) | Batch, full overwrite |
| Ticker universe (~100k+ symbols) | Tiingo | Bulk `supported_tickers.zip` | `tickers_universe` | [batch_tiingo_universe.py](ingest/batch_tiingo_universe.py) | Batch, full overwrite |
| News articles | Tiingo | `/tiingo/news` | `news_raw` | [batch_tiingo_news.py](ingest/batch_tiingo_news.py) | **Disabled**: the free Tiingo plan returns 403 on this endpoint. Batch, incremental MERGE on `article_id` |
| Live trades | Finnhub | WebSocket `wss://ws.finnhub.io` | `quotes_raw` | [stream_finnhub_consumer.py](ingest/stream_finnhub_consumer.py) → [stream_finnhub_autoloader.py](ingest/stream_finnhub_autoloader.py) | Streaming: consumer writes JSON to the `quote_landing` volume, Auto Loader appends it to Bronze |

### FRED series

| Series ID | Indicator | Frequency |
|---|---|---|
| CPIAUCSL | Consumer Price Index (All Urban Consumers) | Monthly |
| UNRATE | Unemployment Rate | Monthly |
| FEDFUNDS | Federal Funds Rate (Effective) | Monthly |
| DGS10 | 10-Year Treasury Yield | Daily |
| DGS2 | 2-Year Treasury Yield | Daily |
| T10Y2Y | 10Y-2Y Treasury Spread | Daily |
| GDP | Gross Domestic Product | Quarterly |
| UMCSENT | U. of Michigan Consumer Sentiment | Monthly |

The descriptions for these series are kept in `beacon.gold.dim_indicator`, loaded by `dbt seed` from [dbt/seeds/dim_indicator.csv](dbt/seeds/dim_indicator.csv). The Bronze table DDL is in [sql/](sql/).
