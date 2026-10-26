-- Calendar from the first watchlist bar to today. A trading day is any day
-- the watchlist has a bar for, which needs no holiday calendar; today stays
-- false until its EOD bar is ingested the next morning.
with trading_days as (
    select distinct obs_date
    from {{ ref('stg_markets') }}
),

days as (
    select explode(sequence(min(obs_date), current_date(), interval 1 day)) as date_day
    from trading_days
),

flagged as (
    select
        d.date_day,
        t.obs_date is not null as is_trading_day
    from days as d
    left join trading_days as t
        on t.obs_date = d.date_day
)

select
    date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    date_format(date_day, 'MMMM') as month_name,
    dayofweek(date_day) as day_of_week,
    date_format(date_day, 'EEEE') as day_name,
    dayofweek(date_day) in (1, 7) as is_weekend,
    is_trading_day,
    date_day = last_day(date_day) as is_month_end,
    date_day = last_day(date_day) and month(date_day) in (3, 6, 9, 12) as is_quarter_end,
    month(date_day) = 12 and day(date_day) = 31 as is_year_end,
    -- Only for finished months; mid-month the latest trading day so far isn't the last one.
    last_day(date_day) < current_date()
        and is_trading_day
        and date_day = max(case when is_trading_day then date_day end)
            over (partition by year(date_day), month(date_day))
        as is_last_trading_day_of_month
from flagged
