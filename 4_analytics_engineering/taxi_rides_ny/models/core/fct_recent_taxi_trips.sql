{{
    config(
        materialized='view',
        schema=resolve_schema_for('core')
    )
}}

select *
from {{ ref('facts_tripdata') }}
where DATE(pickup_datetime) >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DBT_DAYS_BACK", "30")) }}' DAY