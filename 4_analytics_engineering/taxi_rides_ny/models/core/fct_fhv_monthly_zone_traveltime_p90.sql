{{ 
    config(
    materialized='table', 
    schema=resolve_schema_for('core')
    ) 
}}


with trip_duration as (
  select pickup_year
  , pickup_month
  , pickup_locationid
  , dropoff_locationid
--   , pickup_borough
--   , dropoff_borough
  , timestamp_diff(dropoff_datetime, pickup_datetime, second) trip_duration
  from {{ ref('dim_fhv_trips') }}
)
select distinct pickup_year
  , pickup_month
  , pickup_locationid
  , dropoff_locationid
--   , pickup_borough
--   , dropoff_borough
  , percentile_cont(trip_duration, 0.90) over(partition by pickup_year, pickup_month, pickup_locationid, dropoff_locationid) as trip_duration_p90
from trip_duration as td
order by pickup_year desc, pickup_month desc, pickup_locationid, dropoff_locationid