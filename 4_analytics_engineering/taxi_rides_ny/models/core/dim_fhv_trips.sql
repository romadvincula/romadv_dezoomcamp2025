{{
    config(
        materialized='table',
        schema=resolve_schema_for('core')
    )
}}

with fhv as (
    select *,
        'FHV' as service_type
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select fhv.tripid
    , fhv.dispatching_base_num
    , fhv.pickup_locationid
    , pu.borough as pickup_borough
    , pu.zone as pickup_zone
    , fhv.dropoff_locationid
    , drp.borough as dropoff_borough
    , drp.zone as dropoff_zone
    , fhv.pickup_datetime
    , fhv.dropoff_datetime
    -- date columns
    , extract(year from fhv.pickup_datetime) as pickup_year
    , extract(quarter from fhv.pickup_datetime) as pickup_quarter
    , concat(extract(year from fhv.pickup_datetime), '/Q', extract(quarter from pickup_datetime)) as pickup_year_qtr
    , extract(month from fhv.pickup_datetime) as pickup_month
    , concat( extract( year from date_sub( date(pickup_datetime), interval 1 year) ), '/Q', extract( quarter from pickup_datetime) ) as previous_pickup_year_qtr
    , fhv.sr_flag
    , fhv.affiliated_base_number
from fhv
inner join dim_zones as pu
on fhv.pickup_locationid = pu.locationid
inner join dim_zones as drp
on fhv.dropoff_locationid = drp.locationid