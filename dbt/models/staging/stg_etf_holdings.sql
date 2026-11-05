-- Sector ETF holdings, one row per line of each State Street file per
-- as_of_date. is_equity marks lines that are stocks Tiingo lists (the ones
-- batch_tiingo_eod.py --symbols companies prices); cash, money market and
-- index-future lines are false. weight is a fraction (0.15 = 15%).
with universe as (
    select
        upper(ticker) as symbol,
        exchange,
        price_currency
    from {{ source('bronze', 'tickers_universe') }}
    where asset_type = 'Stock'
    -- A ticker can be reused after a delisting; keep the newest listing.
    qualify row_number() over (partition by upper(ticker) order by end_date desc nulls first) = 1
),

deduped as (
    select *
    from {{ source('bronze', 'etf_holdings_raw') }}
    qualify row_number() over (
        partition by etf_symbol, as_of_date, position order by _ingested_at desc
    ) = 1
)

select
    h.etf_symbol,
    h.as_of_date,
    h.position,
    h.holding_name,
    h.ticker,
    h.symbol,
    h.identifier,
    h.sedol,
    h.weight_pct / 100 as weight,
    h.shares_held,
    h.local_currency,
    u.symbol is not null as is_equity,
    u.exchange,
    u.price_currency,
    h._ingested_at
from deduped as h
left join universe as u
    on u.symbol = h.symbol
