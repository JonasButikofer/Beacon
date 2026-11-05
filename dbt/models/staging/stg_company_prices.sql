-- Daily bars for the companies in the sector ETFs, one row per (symbol, obs_date).
-- Same shape and rules as stg_markets: a full rebuild, because Tiingo rewrites
-- adj_* history after every dividend or split (~210 symbols x ~3k days).
with deduped as (
    select *
    from {{ source('bronze', 'company_prices_raw') }}
    qualify row_number() over (partition by symbol, obs_date order by _ingested_at desc) = 1
)

select
    symbol,
    obs_date,
    open,
    high,
    low,
    close,
    volume,
    adj_open,
    adj_high,
    adj_low,
    adj_close,
    adj_volume,
    coalesce(div_cash, 0) as div_cash,
    coalesce(split_factor, 1) as split_factor,
    adj_close / lag(adj_close) over (partition by symbol order by obs_date) - 1 as daily_return,
    _ingested_at
from deduped
