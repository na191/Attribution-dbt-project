-- models/marts/mart_attributed_revenue.sql
-- Core attribution mart: calculates revenue credit per touchpoint
-- for all 5 attribution models, then aggregates by channel/campaign/date.

with touchpoints as (
    select * from {{ ref('int_user_touchpoints') }}
),

-- Calculate raw weights for each model
weighted as (
    select
        session_id,
        user_id,
        event_date,
        channel,
        campaign,
        order_value,
        touchpoint_position,
        total_touchpoints,
        conversion_timestamp,
        event_timestamp,

        -- All 5 models computed in parallel
        {{ attribution_weight('first_touch') }}  as weight_first_touch,
        {{ attribution_weight('last_touch') }}   as weight_last_touch,
        {{ attribution_weight('linear') }}       as weight_linear,
        {{ attribution_weight('time_decay') }}   as weight_time_decay_raw,
        {{ attribution_weight('u_shaped') }}     as weight_u_shaped

    from touchpoints
),

-- Normalize time_decay weights so they sum to 1 per user
normalized as (
    select
        *,
        case
            when sum(weight_time_decay_raw) over (partition by user_id) > 0
            then weight_time_decay_raw / sum(weight_time_decay_raw) over (partition by user_id)
            else 0
        end as weight_time_decay

    from weighted
),

-- Apply weights to order value to get attributed revenue
attributed as (
    select
        session_id,
        user_id,
        event_date,
        channel,
        campaign,
        order_value,

        round(order_value * weight_first_touch, 2)  as revenue_first_touch,
        round(order_value * weight_last_touch, 2)   as revenue_last_touch,
        round(order_value * weight_linear, 2)       as revenue_linear,
        round(order_value * weight_time_decay, 2)   as revenue_time_decay,
        round(order_value * weight_u_shaped, 2)     as revenue_u_shaped

    from normalized
)

-- Final aggregation: revenue by channel, campaign, and date
select
    event_date,
    channel,
    campaign,

    count(distinct user_id)                         as converted_users,
    sum(revenue_first_touch)                        as revenue_first_touch,
    sum(revenue_last_touch)                         as revenue_last_touch,
    sum(revenue_linear)                             as revenue_linear,
    sum(revenue_time_decay)                         as revenue_time_decay,
    sum(revenue_u_shaped)                           as revenue_u_shaped

from attributed
group by 1, 2, 3
order by 1, 2, 3
