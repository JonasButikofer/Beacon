-- 1- and 5-minute OHLCV bars with VWAP from the Finnhub trades. Incremental:
-- each run rebuilds every bar from the latest bar_date onward, so a day that
-- was still streaming last run gets completed rather than duplicated.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['symbol', 'bar_minutes', 'bar_start']
) }}

with trades as (
    select *
    from {{ ref('stg_trades') }}
    {% if is_incremental() %}
    where trade_date >= (select max(bar_date) from {{ this }})
    {% endif %}
),

bucketed as (
    select
        t.symbol,
        i.bar_minutes,
        timestamp_seconds(floor(unix_seconds(t.trade_ts) / (60 * i.bar_minutes)) * 60 * i.bar_minutes)
            as bar_start,
        t.trade_date,
        t.is_regular_session,
        t.trade_ts,
        t.price,
        t.volume
    from trades as t
    cross join (select explode(array(1, 5)) as bar_minutes) as i
)

select
    symbol,
    bar_minutes,
    bar_start,
    min(trade_date) as bar_date,
    min_by(price, trade_ts) as open,
    max(price) as high,
    min(price) as low,
    max_by(price, trade_ts) as close,
    sum(volume) as volume,
    sum(price * volume) / nullif(sum(volume), 0) as vwap,
    count(*) as trade_count,
    bool_and(is_regular_session) as is_regular_session
from bucketed
group by symbol, bar_minutes, bar_start
