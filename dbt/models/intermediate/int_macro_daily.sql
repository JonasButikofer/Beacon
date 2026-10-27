-- Every FRED series on a daily calendar: one row per (date_day, series_id)
-- from the series' first observation to today, carrying the latest observation
-- on or before that day.
--
-- The 3- and 12-month changes are computed per observation first and then
-- carried forward. Doing it on the daily grid instead would pair today's value
-- (often last month's print) with the value from exactly 12 months ago (that
-- month's print), i.e. an 11-month change.
--
-- "As of" means by obs_date, not by release date: September CPI is dated
-- 09-01 but published mid-October, so it shows up here ~6 weeks early.
with obs as (
    select series_id, obs_date, value
    from {{ ref('stg_macro') }}
),

calendar as (
    select series_id, explode(sequence(min(obs_date), current_date(), interval 1 day)) as date_day
    from obs
    group by series_id
),

-- Latest observation on or before each day.
as_of as (
    select
        c.series_id,
        c.date_day,
        last_value(o.obs_date, true) over (
            partition by c.series_id order by c.date_day
            rows between unbounded preceding and current row
        ) as obs_date,
        last_value(o.value, true) over (
            partition by c.series_id order by c.date_day
            rows between unbounded preceding and current row
        ) as value
    from calendar as c
    left join obs as o
        on o.series_id = c.series_id
        and o.obs_date = c.date_day
),

-- Changes per observation. The as-of lookup handles daily series whose date
-- a year back lands on a weekend; monthly and quarterly series hit an exact
-- observation.
obs_changes as (
    select
        o.series_id,
        o.obs_date,
        o.value,
        m3.value as value_3m_ago,
        m12.value as value_12m_ago
    from obs as o
    left join as_of as m3
        on m3.series_id = o.series_id
        and m3.date_day = add_months(o.obs_date, -3)
    left join as_of as m12
        on m12.series_id = o.series_id
        and m12.date_day = add_months(o.obs_date, -12)
)

select
    a.date_day,
    a.series_id,
    a.obs_date,
    datediff(a.date_day, a.obs_date) as days_since_obs,
    c.value,
    c.value_3m_ago,
    c.value_12m_ago,
    c.value - c.value_3m_ago as chg_3m,
    c.value - c.value_12m_ago as chg_12m,
    -- try_divide: rates and spreads (FEDFUNDS, T10Y2Y) have sat at exactly 0.
    (try_divide(c.value, c.value_12m_ago) - 1) * 100 as pct_chg_12m
from as_of as a
inner join obs_changes as c
    on c.series_id = a.series_id
    and c.obs_date = a.obs_date
