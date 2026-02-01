-- models/intermediate/int_user_touchpoints.sql
-- Joins events to conversions, ranks touchpoints per user,
-- and flags first/last/middle positions for attribution logic.

with events as (
    select * from {{ ref('stg_raw_events') }}
),

conversions as (
    select * from {{ ref('stg_conversions') }}
),

-- Only keep touchpoints that occurred BEFORE or AT conversion time
-- This prevents post-conversion noise from polluting the funnel
events_pre_conversion as (
    select
        e.*,
        c.conversion_timestamp,
        c.conversion_date,
        c.order_value,
        case when c.user_id is not null then true else false end as is_converted_user

    from events e
    left join conversions c
        on e.user_id = c.user_id
        and e.event_timestamp <= c.conversion_timestamp
),

-- Rank each touchpoint within a user's journey
ranked as (
    select
        *,
        row_number() over (partition by user_id order by event_timestamp asc) as touchpoint_position,
        count(*) over (partition by user_id) as total_touchpoints

    from events_pre_conversion
    where is_converted_user = true  -- Only converted users for attribution
)

select
    session_id,
    user_id,
    event_timestamp,
    event_date,
    channel,
    campaign,
    device,
    page_visited,
    session_duration_seconds,
    conversion_timestamp,
    conversion_date,
    order_value,
    touchpoint_position,
    total_touchpoints,

    -- Position flags
    case when touchpoint_position = 1 then true else false end as is_first_touch,
    case when touchpoint_position = total_touchpoints then true else false end as is_last_touch,
    case
        when touchpoint_position = 1 then 'first'
        when touchpoint_position = total_touchpoints then 'last'
        else 'middle'
    end as touchpoint_type

from ranked
