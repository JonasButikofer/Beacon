-- One row per watchlist symbol: Tiingo metadata, the universe's currency, the
-- price history range in stg_markets, and the symbol's role in the watchlist.
-- Roles mirror the watchlist table in README.md; add a line here when a
-- symbol is added to the ingest jobs.
with universe as (
    select upper(ticker) as symbol, price_currency
    from {{ source('bronze', 'tickers_universe') }}
    qualify row_number() over (partition by upper(ticker) order by end_date desc nulls first) = 1
),

price_range as (
    select symbol, min(obs_date) as first_price_date, max(obs_date) as last_price_date
    from {{ ref('stg_markets') }}
    group by symbol
)

select
    t.symbol,
    t.name,
    t.description,
    t.asset_type,
    t.exchange_code,
    u.price_currency,
    case
        when t.symbol in ('SPY', 'QQQ', 'DIA') then 'broad_market'
        when t.symbol in ('XLF', 'XLE', 'XLK', 'XLU') then 'sector'
        when t.symbol = 'IWM' then 'size'
        when t.symbol = 'TLT' then 'bond'
        else 'other'
    end as role,
    case t.symbol
        when 'SPY' then 'S&P 500'
        when 'QQQ' then 'Nasdaq 100'
        when 'DIA' then 'Dow Jones Industrial Average'
        when 'IWM' then 'Russell 2000 small caps'
        when 'XLF' then 'Financials'
        when 'XLE' then 'Energy'
        when 'XLK' then 'Technology'
        when 'XLU' then 'Utilities'
        when 'TLT' then '20+ year Treasuries'
    end as segment,
    t.symbol = 'SPY' as is_benchmark,
    p.first_price_date,
    p.last_price_date
from {{ source('bronze', 'tickers_raw') }} as t
left join universe as u
    on u.symbol = t.symbol
left join price_range as p
    on p.symbol = t.symbol
