-- Per-ETF daily price health: returns, trend, volatility and drawdown.
-- The logic lives in macros/daily_price_health.sql, shared with
-- mart_company_daily so ETFs and companies are measured the same way.
{{ daily_price_health(ref('stg_markets')) }}
