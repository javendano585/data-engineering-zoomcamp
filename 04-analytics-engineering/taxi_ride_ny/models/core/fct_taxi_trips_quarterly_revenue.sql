{{
    config(
        materialized='table'
    )
}}

WITH quarterly_rev AS (
  SELECT 
    EXTRACT(year FROM pickup_datetime) AS rev_year,
    EXTRACT(quarter FROM pickup_datetime) AS rev_quarter,
    EXTRACT(year FROM pickup_datetime) || '-Q' || EXTRACT(quarter FROM pickup_datetime) AS quarter, 
    service_type,
    COUNT(*) AS num_trips,
    SUM(total_amount) AS revenue
  FROM {{ ref('fact_trips') }}
  GROUP BY ALL
)
SELECT 
  a.service_type,
  a.quarter,
  (a.revenue / b.revenue) - 1 AS yoy_growth 
FROM quarterly_rev a
LEFT JOIN quarterly_rev b 
  ON a.rev_quarter = b.rev_quarter 
    AND a.rev_year = (b.rev_year + 1)
    AND a.service_type = b.service_type