FROM python:3.9.1

RUN apt-get install wget
COPY requirements.txt requirements.txt 
RUN pip install pandas sqlalchemy psycopg2 pyarrow prefect
# RUN pip install -r requirements.txt

WORKDIR /app
COPY ingest_data.py ingest_data.py 

ENTRYPOINT [ "python", "ingest_data.py" ]