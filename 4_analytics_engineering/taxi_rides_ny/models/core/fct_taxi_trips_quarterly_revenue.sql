{{ 
    config(
    materialized='table', 
    schema=resolve_schema_for('core')
    ) 
}}


with qtr_yoy as (
    select pickup_year_qtr
    , previous_pickup_year_qtr
    , service_type
    , sum(total_amount) as revenue
    from {{ ref('facts_tripdata') }}
    group by pickup_year_qtr
    , previous_pickup_year_qtr
    , service_type
), 
t1 as (
    select qtr_yoy.pickup_year_qtr
    , qtr_yoy.revenue as current_revenue
    , qtr_yoy.previous_pickup_year_qtr
    , last_year.revenue as previous_revenue
    , qtr_yoy.service_type
    from qtr_yoy
    left join qtr_yoy as last_year 
        on (qtr_yoy.previous_pickup_year_qtr = last_year.pickup_year_qtr and qtr_yoy.service_type = last_year.service_type)
)
select service_type
    , pickup_year_qtr
    , current_revenue
    , previous_pickup_year_qtr
    , previous_revenue
    , ((current_revenue - previous_revenue) / previous_revenue)*100 as revenue_growth
from t1
order by service_type, pickup_year_qtr DESC