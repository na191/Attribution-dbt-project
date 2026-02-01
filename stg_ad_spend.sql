-- models/staging/stg_ad_spend.sql
-- Cleans the daily ad spend table

select
    cast(spend_date as date) as spend_date,
    lower(trim(channel)) as channel,
    lower(trim(campaign)) as campaign,
    cast(spend_usd as float) as spend_usd,
    cast(impressions as integer) as impressions,

    -- Derived: CPM
    case
        when impressions > 0 then round((spend_usd / impressions) * 1000, 2)
        else null
    end as cpm

from {{ source('raw', 'ad_spend') }}

where spend_usd > 0
