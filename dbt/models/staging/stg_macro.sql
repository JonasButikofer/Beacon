-- FRED observations, one row per (series_id, obs_date), labelled from the
-- dim_indicator seed. Left join so an untracked series still shows up (and
-- fails the relationships test) instead of silently disappearing.
select
    m.series_id,
    m.obs_date,
    m.value,
    i.indicator_name,
    i.category,
    i.frequency,
    i.units,
    m._ingested_at
from {{ source('bronze', 'macro_raw') }} as m
left join {{ ref('dim_indicator') }} as i
    on m.series_id = i.series_id
where m.value is not null
