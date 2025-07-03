{{ 
    config(
    materialized='table', 
    schema=resolve_schema_for('core')
    ) 
}}


with filtered as (
    select service_type
        , pickup_year
        , pickup_month
        , fare_amount
    from {{ ref('facts_tripdata') }}
    where fare_amount > 0
    and trip_distance > 0
    and payment_type_description in ('Cash', 'Credit card')
)
select distinct service_type
    , pickup_year
    , pickup_month
    , percentile_cont(fare_amount, 0.97) over(partition by service_type, pickup_year, pickup_month) as p97
    , percentile_cont(fare_amount, 0.95) over(partition by service_type, pickup_year, pickup_month) as p95
    , percentile_cont(fare_amount, 0.90) over(partition by service_type, pickup_year, pickup_month) as p90
from filtered as f
order by service_type
, pickup_year desc
, pickup_month desc
