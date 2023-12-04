## Model deployment
[Tutorial](https://cloud.google.com/bigquery-ml/docs/export-model-tutorial)
### Steps
- gcloud auth login
- bq --project_id dtc-de-course-2023-406120 extract -m trips_data_all.tip_model gs://dtc_data_lake_dtc-de-course-2023-406120/tip_model
- mkdir tmp\model
- gsutil cp -r gs://dtc_data_lake_dtc-de-course-2023-406120/tip_model tmp\model 
- mkdir serving_dir\tip_model\1
<!-- - cp -r tmp\model\tip_model\* serving_dir\tip_model\1 -->
- xcopy /e tmp\model\tip_model\* serving_dir\tip_model\1 
- docker pull tensorflow/serving
- docker run -p 8501:8501 --mount type=bind,source="C:\Users\jaime.avendano\Documents\DE\data-engineering-zoomcamp\week_3_data_warehouse\serving_dir\tip_model",target="C:\Users\jaime.avendano\Documents\DE\data-engineering-zoomcamp\week_3_data_warehouse\models\tip_model" -e MODEL_NAME=tip_model -t tensorflow/serving &
- curl -d '{"instances": [{"passenger_count":1, "trip_distance":12.2, "PULocationID":"193", "DOLocationID":"264", "payment_type":"2","fare_amount":20.4,"tolls_amount":0.0}]}' -X POST http://localhost:8501/v1/models/tip_model:predict
- http://localhost:8501/v1/models/tip_model