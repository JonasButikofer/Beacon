-- Parent -> child edges for drilling down from the market to one company:
--   level 1: SPY -> the sector, small-cap and bond ETFs (the same set
--            mart_sector_relative_strength compares to SPY)
--   level 2: sector ETF -> the companies in its latest holdings snapshot
-- mart_drilldown_relative_strength works on any edge, so a new level (e.g.
-- industries between sector and company) only needs new rows here.
with latest_snapshot as (
    select etf_symbol, max(as_of_date) as as_of_date
    from {{ ref('stg_etf_holdings') }}
    group by etf_symbol
),

edges as (
    select
        'SPY' as parent_symbol,
        symbol as child_symbol,
        1 as level,
        cast(null as double) as weight,
        segment as child_name
    from {{ ref('dim_symbol') }}
    where role in ('sector', 'size', 'bond')

    union all

    select
        h.etf_symbol as parent_symbol,
        h.symbol as child_symbol,
        2 as level,
        h.weight,
        c.name as child_name
    from {{ ref('stg_etf_holdings') }} as h
    inner join latest_snapshot as l
        on l.etf_symbol = h.etf_symbol
        and l.as_of_date = h.as_of_date
    left join {{ ref('dim_company') }} as c
        on c.symbol = h.symbol
    where h.is_equity
),

parents as (
    select distinct parent_symbol
    from edges
)

select
    e.parent_symbol,
    e.child_symbol,
    e.level,
    e.weight,
    e.child_name,
    p.parent_symbol is not null as has_children
from edges as e
left join parents as p
    on p.parent_symbol = e.child_symbol
