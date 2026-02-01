-- models/staging/stg_conversions.sql
-- Cleans the conversions table; filters to converted users only

select
    user_id,
    cast(conversion_timestamp as TIMESTAMP) as conversion_timestamp,
    cast(conversion_timestamp as DATE) as conversion_date,
    cast(order_value as FLOAT64) as order_value

from {{ source('raw', 'conversions') }}

where converted = true
  and conversion_timestamp is not null
  and order_value is not null