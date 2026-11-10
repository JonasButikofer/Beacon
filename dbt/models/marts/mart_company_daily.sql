-- Per-company daily price health: returns, trend, volatility and drawdown,
-- computed exactly like mart_market_daily (macros/daily_price_health.sql).
-- Covers every company with prices in stg_company_prices, including ones that
-- have since left their sector ETF (see dim_company.is_current_holding).
{{ daily_price_health(ref('stg_company_prices')) }}
