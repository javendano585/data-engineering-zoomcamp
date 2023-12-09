{{ config(materialized='view') }}

select
    -- identifiers
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(affiliated_base_number as string) as affiliated_base_number,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    cast(sr_flag as integer) as sr_flag


from {{ source('staging', 'fhv_tripdata') }}

-- dbt run --select stg_fhv_tripdata --vars '{"is_test_run": false}'

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
