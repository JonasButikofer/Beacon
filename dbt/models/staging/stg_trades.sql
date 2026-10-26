-- Finnhub trades, deduplicated. Finnhub sends no trade id, so trade_key hashes
-- every field: a consumer that writes the same message twice collapses to one
-- row. The trade-off is that two genuinely identical prints (same symbol, ms,
-- price, size and conditions) also collapse, which is rare and moves VWAP
-- negligibly.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='trade_key'
) }}

with src as (
    select
        sha2(concat_ws('|', symbol, cast(trade_time as string), cast(price as string),
                       cast(volume as string), array_join(conditions, ',')), 256) as trade_key,
        symbol,
        price,
        volume,
        conditions,
        trade_ts,
        _ingested_at
    from {{ source('bronze', 'quotes_raw') }}
    {% if is_incremental() %}
    where _ingested_at > (select max(_ingested_at) from {{ this }})
    {% endif %}
),

deduped as (
    select *
    from src
    qualify row_number() over (partition by trade_key order by _ingested_at) = 1
)

select
    trade_key,
    symbol,
    price,
    volume,
    conditions,
    trade_ts,
    to_date(from_utc_timestamp(trade_ts, 'America/New_York')) as trade_date,
    date_format(from_utc_timestamp(trade_ts, 'America/New_York'), 'HH:mm') between '09:30' and '15:59'
        as is_regular_session,
    _ingested_at
from deduped
