{{
    config(
        materialized='view',
        schema=resolve_schema_for('stg')
    )
}}

with tripdata as 
(
    select *,
    from {{ source('staging','external_fhv_nytaxi') }}
    where dispatching_base_num is not null
)
    -- identifiers
    select {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    -- cast(SR_Flag as numeric) as sr_flag,
    cast(sr_flag as numeric) as sr_flag,
    Affiliated_base_number as affiliated_base_number,

from tripdata

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit {{ env_var('DBT_STG_ROW_LIMITER', '100') }}

{% endif %}