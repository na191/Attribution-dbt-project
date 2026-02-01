-- models/staging/stg_raw_events.sql
-- Cleans and casts the raw marketing event log

select
    session_id,
    user_id,
    cast(event_timestamp as TIMESTAMP) as event_timestamp,
    lower(trim(channel)) as channel,
    lower(trim(campaign)) as campaign,
    lower(trim(device)) as device,
    trim(page_visited) as page_visited,
    cast(session_duration_seconds as INT64) as session_duration_seconds,

    -- Derived fields
    cast(event_timestamp as date) as event_date,
    extract(hour from cast(event_timestamp as TIMESTAMP)) as event_hour,
    extract(dayofweek from cast(event_timestamp as TIMESTAMP)) as event_dow

from {{ source('raw', 'raw_events') }}

where session_id is not null
  and user_id is not null
  and event_timestamp is not null