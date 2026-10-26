-- Each ETF trading day next to the macro backdrop as of that day, plus simple
-- regime labels. fwd_21d_return looks ahead about a month so you can ask
-- "what did SPY do next when the curve was inverted?". Macro values are as of
-- obs_date (see int_macro_daily), so they lead their real release dates.
with macro as (
    select
        date_day,
        max(case when series_id = 'DGS10' then value end) as dgs10,
        max(case when series_id = 'DGS2' then value end) as dgs2,
        max(case when series_id = 'T10Y2Y' then value end) as t10y2y,
        max(case when series_id = 'FEDFUNDS' then value end) as fedfunds,
        max(case when series_id = 'FEDFUNDS' then chg_3m end) as fedfunds_chg_3m,
        max(case when series_id = 'CPIAUCSL' then pct_chg_12m end) as cpi_yoy_pct,
        max(case when series_id = 'CPIAUCSL' then (pow(value / value_3m_ago, 4) - 1) * 100 end)
            as cpi_3m_ann_pct,
        max(case when series_id = 'UNRATE' then value end) as unrate,
        max(case when series_id = 'UNRATE' then chg_3m end) as unrate_chg_3m,
        max(case when series_id = 'UMCSENT' then value end) as umcsent,
        max(case when series_id = 'GDP' then pct_chg_12m end) as gdp_yoy_pct
    from {{ ref('int_macro_daily') }}
    group by date_day
),

markets as (
    select
        symbol,
        obs_date,
        adj_close,
        daily_return,
        lead(adj_close, 21) over (partition by symbol order by obs_date) / adj_close - 1 as fwd_21d_return
    from {{ ref('mart_market_daily') }}
)

select
    m.obs_date,
    m.symbol,
    m.adj_close,
    m.daily_return,
    m.fwd_21d_return,
    x.dgs10,
    x.dgs2,
    x.t10y2y,
    x.t10y2y < 0 as curve_inverted,
    x.fedfunds,
    x.fedfunds_chg_3m,
    case
        when x.fedfunds_chg_3m > 0.25 then 'hiking'
        when x.fedfunds_chg_3m < -0.25 then 'cutting'
        when x.fedfunds_chg_3m is not null then 'holding'
    end as fed_regime,
    x.cpi_yoy_pct,
    x.cpi_3m_ann_pct,
    -- Recent 3 months running hotter or cooler than the past year.
    case
        when x.cpi_3m_ann_pct > x.cpi_yoy_pct + 0.5 then 'rising'
        when x.cpi_3m_ann_pct < x.cpi_yoy_pct - 0.5 then 'falling'
        when x.cpi_3m_ann_pct is not null and x.cpi_yoy_pct is not null then 'stable'
    end as inflation_trend,
    x.unrate,
    x.unrate_chg_3m,
    x.umcsent,
    x.gdp_yoy_pct
from markets as m
left join macro as x
    on x.date_day = m.obs_date
