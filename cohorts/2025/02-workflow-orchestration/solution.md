## Module 2 Homework

1. Size of `yellow_tripdata_2020-12.csv` =  128.3 MB

2. `green_tripdata_2020-04.csv`

3. yellow 2020 count = 24,648,499
``` 
  SELECT COUNT(*)  
  FROM `dtc-de-course-2025-449816.de_zoomcamp.yellow_tripdata`  
  WHERE filename LIKE '%2020%';  
```

4. green 2020 count = 1,734,051
```
SELECT COUNT(*)
FROM `dtc-de-course-2025-449816.de_zoomcamp.green_tripdata`
WHERE filename LIKE '%2020%';
```

5. yellow march 2021 count = 1,925,152
```
SELECT COUNT(*)
FROM `dtc-de-course-2025-449816.de_zoomcamp.yellow_tripdata`
WHERE filename = 'yellow_tripdata_2021-03.csv';
```