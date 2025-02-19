1. dbt model  
d. select * from myproject.my_nyc_tripdata.ext_green_taxi

2. Dynamic Dates
d. Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY

3. Lineage  
e. dbt run --select models/staging/+  

4. macros and jinja  
- When using core, it materializes in the dataset defined in DBT_BIGQUERY_TARGET_DATASET
- When using stg, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET
- When using staging, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET

5. YoY Growth  
Green: Best Q1, Worst Q2  
Yellow: Best Q1, Worth Q2

6. Percentiles  
e. Green: 28, 23, 18  
Yellow: 32, 26.5, 19.5

```
WITH base AS (
  SELECT 
    service_type,
    EXTRACT(year FROM pickup_datetime) AS year,
    EXTRACT(month FROM pickup_datetime) AS month,
    fare_amount
  FROM `de_zoomcamp.fact_trips`
  WHERE fare_amount > 0
    AND trip_distance > 0
    AND payment_type_description in ('Cash', 'Credit Card')
    AND EXTRACT(year FROM pickup_datetime) IN (2019, 2020)

)
SELECT DISTINCT
  service_type,
  year, 
  month,
  PERCENTILE_CONT(fare_amount, .90) OVER (PARTITION BY year, month, service_type) AS p90,
  PERCENTILE_CONT(fare_amount, .95) OVER (PARTITION BY year, month, service_type) AS p95,
  PERCENTILE_CONT(fare_amount, .97) OVER (PARTITION BY year, month, service_type) AS p97
FROM base
WHERE year = 2020 AND month = 4
LIMIT 100;
```

7. FHV trip rank  
a. LaGuardia, Chinatown, Garment

```
WITH ts_diff AS (
  SELECT 
    *,
    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, second) AS timestamp_diff
  FROM `de_zoomcamp.dim_fhv_trips`
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
WHERE year = 2019 
  AND month = 11
  AND pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
  AND dropoff_trip_rank <= 2
  ;
```