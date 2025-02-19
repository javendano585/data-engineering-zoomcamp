{{
    config(
        materialized='table'
    )
}}

WITH ts_diff AS (
  SELECT 
    *,
    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, second) AS timestamp_diff
  FROM {{ ref('dim_fhv_trips') }}
),
p90s AS (
  SELECT DISTINCT
    year,
    month,
    pickup_zone,
    dropoff_zone,
    PERCENTILE_CONT(timestamp_diff, .90) OVER (PARTITION BY year, month, pickup_zone, dropoff_zone) AS p90,
  FROM ts_diff
),
ranked AS (
  SELECT 
    *,
    RANK() OVER(PARTITION BY year, month, pickup_zone ORDER BY p90 DESC) AS dropoff_trip_rank
  FROM p90s
)
SELECT *
FROM ranked