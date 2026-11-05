-- One row per company that has appeared in a sector ETF's holdings, labelled
-- from the latest snapshot that held it. is_current_holding = it's in its
-- ETF's latest snapshot; only current holdings get new prices, so a company
-- that left keeps its history but stops updating.
with stocks as (
    select *
    from {{ ref('stg_etf_holdings') }}
    where is_equity
),

latest_snapshot as (
    select etf_symbol, max(as_of_date) as as_of_date
    from {{ ref('stg_etf_holdings') }}
    group by etf_symbol
),

last_held as (
    select
        symbol,
        etf_symbol,
        holding_name,
        weight,
        as_of_date,
        exchange,
        price_currency,
        min(as_of_date) over (partition by symbol) as first_held_date
    from stocks
    -- Latest snapshot first; if two ETFs held it that day, the bigger weight wins.
    qualify row_number() over (partition by symbol order by as_of_date desc, weight desc) = 1
),

price_range as (
    select symbol, min(obs_date) as first_price_date, max(obs_date) as last_price_date
    from {{ ref('stg_company_prices') }}
    group by symbol
)

select
    h.symbol,
    initcap(h.holding_name) as name,
    h.etf_symbol as sector_etf,
    s.segment as sector,
    h.exchange,
    h.price_currency,
    h.weight as latest_weight,
    h.first_held_date,
    h.as_of_date as last_held_date,
    h.as_of_date = l.as_of_date as is_current_holding,
    p.first_price_date,
    p.last_price_date
from last_held as h
left join latest_snapshot as l
    on l.etf_symbol = h.etf_symbol
left join {{ ref('dim_symbol') }} as s
    on s.symbol = h.etf_symbol
left join price_range as p
    on p.symbol = h.symbol
