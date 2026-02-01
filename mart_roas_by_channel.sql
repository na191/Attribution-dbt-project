-- models/marts/mart_roas_by_channel.sql
-- Joins attributed revenue to actual ad spend to calculate ROAS
-- across all attribution models. This is the key "so what" table.

with attributed_revenue as (
    select
        event_date,
        channel,
        campaign,
        sum(revenue_first_touch) as revenue_first_touch,
        sum(revenue_last_touch)  as revenue_last_touch,
        sum(revenue_linear)      as revenue_linear,
        sum(revenue_time_decay)  as revenue_time_decay,
        sum(revenue_u_shaped)    as revenue_u_shaped,
        sum(converted_users)     as converted_users

    from {{ ref('mart_attributed_revenue') }}
    group by 1, 2, 3
),

ad_spend as (
    select
        spend_date,
        channel,
        campaign,
        sum(spend_usd)     as total_spend,
        sum(impressions)   as total_impressions

    from {{ ref('stg_ad_spend') }}
    group by 1, 2, 3
),

-- Full outer join: some channels (organic, direct) have no spend
joined as (
    select
        coalesce(ar.event_date, asp.spend_date)     as report_date,
        coalesce(ar.channel, asp.channel)           as channel,
        coalesce(ar.campaign, asp.campaign)         as campaign,

        ar.converted_users,
        coalesce(asp.total_spend, 0)                as total_spend,
        coalesce(asp.total_impressions, 0)          as total_impressions,

        coalesce(ar.revenue_first_touch, 0)         as revenue_first_touch,
        coalesce(ar.revenue_last_touch, 0)          as revenue_last_touch,
        coalesce(ar.revenue_linear, 0)              as revenue_linear,
        coalesce(ar.revenue_time_decay, 0)          as revenue_time_decay,
        coalesce(ar.revenue_u_shaped, 0)            as revenue_u_shaped

    from attributed_revenue ar
    full outer join ad_spend asp
        on ar.event_date = asp.spend_date
        and ar.channel = asp.channel
        and ar.campaign = asp.campaign
)

select
    report_date,
    channel,
    campaign,
    converted_users,
    total_spend,
    total_impressions,

    revenue_first_touch,
    revenue_last_touch,
    revenue_linear,
    revenue_time_decay,
    revenue_u_shaped,

    -- ROAS for each model (null if no spend to avoid div/0)
    case when total_spend > 0 then round(revenue_first_touch / total_spend, 2) else null end as roas_first_touch,
    case when total_spend > 0 then round(revenue_last_touch / total_spend, 2)  else null end as roas_last_touch,
    case when total_spend > 0 then round(revenue_linear / total_spend, 2)      else null end as roas_linear,
    case when total_spend > 0 then round(revenue_time_decay / total_spend, 2)  else null end as roas_time_decay,
    case when total_spend > 0 then round(revenue_u_shaped / total_spend, 2)    else null end as roas_u_shaped,

    -- CPM and CPA (based on linear attribution as a neutral baseline)
    case when total_impressions > 0 then round((total_spend / total_impressions) * 1000, 2) else null end as cpm,
    case when converted_users > 0 then round(total_spend / converted_users, 2) else null end as cpa_linear

from joined
order by report_date, channel, campaign
