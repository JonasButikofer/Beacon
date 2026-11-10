-- Every child in dim_symbol_hierarchy against its parent over ~1, 3 and 6
-- months: sectors vs SPY (level 1), companies vs their sector ETF (level 2).
-- rel_* is the child's excess return over the parent; rank_* is 1 for the
-- strongest child of that parent that day.
--
-- Companies refresh in rotation on the free Tiingo plan (see
-- ingest/batch_tiingo_eod.py), so on the newest dates only some children have
-- a row. Compare children_reporting to children_priced and read ranks from the
-- latest date where they match.
--
-- Survivorship bias: level 2 applies today's holdings to every past date, so
-- a company's history counts only if the ETF holds it now. Fine for "who has
-- led this sector lately", not for backtests.
with edges as (
    select parent_symbol, child_symbol, level, weight, has_children
    from {{ ref('dim_symbol_hierarchy') }}
),

returns as (
    select symbol, obs_date, return_1m, return_3m, return_6m
    from {{ ref('int_symbol_returns') }}
),

health as (
    select symbol, obs_date, above_sma_200, drawdown
    from {{ ref('mart_market_daily') }}
    union all
    select symbol, obs_date, above_sma_200, drawdown
    from {{ ref('mart_company_daily') }}
),

relative as (
    select
        e.parent_symbol,
        e.child_symbol,
        c.obs_date,
        e.level,
        e.weight,
        e.has_children,
        c.return_1m,
        c.return_3m,
        c.return_6m,
        p.return_1m as parent_return_1m,
        p.return_3m as parent_return_3m,
        p.return_6m as parent_return_6m,
        c.return_1m - p.return_1m as rel_1m,
        c.return_3m - p.return_3m as rel_3m,
        c.return_6m - p.return_6m as rel_6m,
        h.above_sma_200,
        h.drawdown
    from edges as e
    inner join returns as c
        on c.symbol = e.child_symbol
    inner join returns as p
        on p.symbol = e.parent_symbol
        and p.obs_date = c.obs_date
    left join health as h
        on h.symbol = e.child_symbol
        and h.obs_date = c.obs_date
),

priced as (
    select parent_symbol, count(distinct child_symbol) as children_priced
    from relative
    group by parent_symbol
)

select
    r.*,
    count(*) over (partition by r.parent_symbol, r.obs_date) as children_reporting,
    p.children_priced,
    case when r.rel_1m is not null then rank() over (
        partition by r.parent_symbol, r.obs_date, r.rel_1m is null order by r.rel_1m desc
    ) end as rank_1m,
    case when r.rel_3m is not null then rank() over (
        partition by r.parent_symbol, r.obs_date, r.rel_3m is null order by r.rel_3m desc
    ) end as rank_3m,
    case when r.rel_6m is not null then rank() over (
        partition by r.parent_symbol, r.obs_date, r.rel_6m is null order by r.rel_6m desc
    ) end as rank_6m
from relative as r
inner join priced as p
    on p.parent_symbol = r.parent_symbol
