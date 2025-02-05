## Module 3 Homework

Q1. Count of 2024 yellow = 20,332,093
```
Used details page of yellow_tripdata_2024 table
```

Q2. PULocationID Distinct count = 0 MB for the External Table and 155.12 MB for the Materialized Table
```
SELECT COUNT(DISTINCT PULocationID)
FROM dtc-de-course-2025-449816.de_zoomcamp.external_yellow_tripdata_2024;
-- This query will process 0 B when run.

SELECT COUNT(DISTINCT PULocationID)
FROM dtc-de-course-2025-449816.de_zoomcamp.yellow_tripdata_2024;
-- This query will process 155.12 MB when run.
```


Q3. PULocationID vs PULocationID & DOLocationID  
```
BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
```

Q4. fare_amount of 0 = 8,333
```
SELECT COUNT(1)
FROM dtc-de-course-2025-449816.de_zoomcamp.yellow_tripdata_2024
WHERE fare_amount = 0;
```

Q5. Best strategy to optimize.
```
Partition by tpep_dropoff_datetime and Partition by VendorID
```

Q6. Distinct Vendors between 3/1 and 3/15 = 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
```
SELECT DISTINCT VendorID
FROM dtc-de-course-2025-449816.de_zoomcamp.yellow_tripdata_2024
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
-- bytes processed = 310.24 MB

SELECT DISTINCT VendorID
FROM dtc-de-course-2025-449816.de_zoomcamp.yellow_tripdata_2024
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
-- bytes processed = 26.85 MB
'''

Q7. Where is data stored = GCP Bucket  

Q8. Always cluster = False

Q9. SELECT Count(*)
```
The estimate shows: "This query will process 0 B when run."
Since we're not doing any filtering, it knows it can just use the table count stored in the information schema.
```