-- Per-ETF daily price health: returns, trend, volatility and drawdown.
-- Each indicator stays null until the symbol has enough history for its
-- window, so early rows don't show averages over a handful of days.
with prices as (
    select
        symbol,
        obs_date,
        close,
        adj_close,
        volume,
        daily_return,
        row_number() over (partition by symbol order by obs_date) as day_n
    from {{ ref('stg_markets') }}
),

windows as (
    select
        *,
        case when day_n >= 50 then avg(adj_close) over (
            partition by symbol order by obs_date rows between 49 preceding and current row
        ) end as sma_50,
        case when day_n >= 200 then avg(adj_close) over (
            partition by symbol order by obs_date rows between 199 preceding and current row
        ) end as sma_200,
        -- day 1 has no return, so 20 returns need 21 days
        case when day_n >= 21 then stddev_samp(daily_return) over (
            partition by symbol order by obs_date rows between 19 preceding and current row
        ) * sqrt(252) end as vol_20d_ann,
        max(adj_close) over (
            partition by symbol order by obs_date rows between unbounded preceding and current row
        ) as running_peak
    from prices
),

with_prev as (
    select
        *,
        lag(sma_50) over (partition by symbol order by obs_date) as prev_sma_50,
        lag(sma_200) over (partition by symbol order by obs_date) as prev_sma_200
    from windows
)

select
    symbol,
    obs_date,
    close,
    adj_close,
    volume,
    daily_return,
    sma_50,
    sma_200,
    adj_close > sma_200 as above_sma_200,
    case
        when prev_sma_50 <= prev_sma_200 and sma_50 > sma_200 then 'golden_cross'
        when prev_sma_50 >= prev_sma_200 and sma_50 < sma_200 then 'death_cross'
    end as cross_signal,
    vol_20d_ann,
    running_peak,
    adj_close / running_peak - 1 as drawdown
from with_prev
