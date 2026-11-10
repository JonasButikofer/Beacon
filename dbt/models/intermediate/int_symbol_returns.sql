-- Trailing returns over ~1, 3 and 6 months (21, 63, 126 trading days) for
-- every priced symbol: the watchlist ETFs and the companies in the sector
-- ETFs. Same math as mart_sector_relative_strength; feeds
-- mart_drilldown_relative_strength, which compares each child to its parent.
with prices as (
    select symbol, obs_date, adj_close
    from {{ ref('stg_markets') }}
    union all
    select symbol, obs_date, adj_close
    from {{ ref('stg_company_prices') }}
)

select
    symbol,
    obs_date,
    adj_close,
    adj_close / lag(adj_close, 21) over (partition by symbol order by obs_date) - 1 as return_1m,
    adj_close / lag(adj_close, 63) over (partition by symbol order by obs_date) - 1 as return_3m,
    adj_close / lag(adj_close, 126) over (partition by symbol order by obs_date) - 1 as return_6m
from prices
