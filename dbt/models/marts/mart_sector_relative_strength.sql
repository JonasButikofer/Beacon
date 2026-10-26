-- Sector, small-cap and bond ETFs against SPY over ~1, 3 and 6 months
-- (21, 63, 126 trading days). rel_* is the excess return over SPY; rank_* is
-- 1 for the strongest symbol that day, among symbols with enough history.
with returns as (
    select
        symbol,
        obs_date,
        adj_close / lag(adj_close, 21) over (partition by symbol order by obs_date) - 1 as return_1m,
        adj_close / lag(adj_close, 63) over (partition by symbol order by obs_date) - 1 as return_3m,
        adj_close / lag(adj_close, 126) over (partition by symbol order by obs_date) - 1 as return_6m
    from {{ ref('stg_markets') }}
),

spy as (
    select obs_date, return_1m, return_3m, return_6m
    from returns
    where symbol = 'SPY'
),

relative as (
    select
        r.obs_date,
        r.symbol,
        r.return_1m,
        r.return_3m,
        r.return_6m,
        s.return_1m as spy_return_1m,
        s.return_3m as spy_return_3m,
        s.return_6m as spy_return_6m,
        r.return_1m - s.return_1m as rel_1m,
        r.return_3m - s.return_3m as rel_3m,
        r.return_6m - s.return_6m as rel_6m
    from returns as r
    inner join {{ ref('dim_symbol') }} as d
        on d.symbol = r.symbol
    inner join spy as s
        on s.obs_date = r.obs_date
    where d.role in ('sector', 'size', 'bond')
)

select
    *,
    case when rel_1m is not null then rank() over (partition by obs_date, rel_1m is null order by rel_1m desc) end
        as rank_1m,
    case when rel_3m is not null then rank() over (partition by obs_date, rel_3m is null order by rel_3m desc) end
        as rank_3m,
    case when rel_6m is not null then rank() over (partition by obs_date, rel_6m is null order by rel_6m desc) end
        as rank_6m
from relative
