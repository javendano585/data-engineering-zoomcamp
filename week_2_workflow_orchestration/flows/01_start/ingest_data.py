
import os

import pandas as pd
import pyarrow.parquet as pq

from sqlalchemy import create_engine
from datetime import timedelta
from time import time

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

# @task(log_prints=True)
def transform_data(df):
    print(f'pre:missing passenger count: {df.passenger_count.isin([0]).sum()}')
    df = (
        df
        .assign(
            passenger_count = lambda _df: _df.passenger_count.astype('Int64'),
            RatecodeID = lambda _df: _df.RatecodeID.astype('Int64'),
            tpep_pickup_datetime = lambda _df: pd.to_datetime(_df.tpep_pickup_datetime),
            tpep_dropoff_datetime = lambda _df: pd.to_datetime(_df.tpep_dropoff_datetime)
            )
        .query('passenger_count != 0')
    )
    print(f'post:missing passenger count: {df.passenger_count.isin([0]).sum()}')
    return df

@task(log_prints=True)
def load_parquet_file(file_name, table_name, engine):
    file_initialized = False

    parquet_file = pq.ParquetFile(file_name)
    for batch in parquet_file.iter_batches():
        t_start = time()
        batch_df = transform_data(batch.to_pandas())
        if not file_initialized:
            batch_df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
            file_initialized = True
        
        batch_df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()
        print(f'Inserted another chunk of size {batch_df.shape}, took {(t_end - t_start):.3f} seconds.')
        break

@task(log_prints=True)
def load_csv_file(file_name, table_name, engine):
    df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)
    df = transform_data(next(df_iter))

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    # while True: 
    #     try:
    #         t_start = time()
            
    #         df = next(df_iter)
    #         df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #         df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #         df.to_sql(name=table_name, con=engine, if_exists='append')

    #         t_end = time()

    #         print('inserted another chunk, took %.3f second' % (t_end - t_start))

    #     except StopIteration:
    #         print("Finished ingesting data into the postgres database")
    #         break 

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_file_data(url):
    if url.endswith('.parquet'):
        file_type = 'parquet'
        file_name = 'output.parquet'
    elif url.endswith('.csv.gz'):
        file_type = 'csv'
        file_name = 'output.csv.gz'
    else:
        file_type = 'csv'
        file_name = 'output.csv'

    os.system(f'wget {url} -O {file_name}')
    return file_type, file_name

# @task(log_prints=True, retries=3)
# def prepare_engine(user, password, host, port, db):
#     return create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f'Logging Subflow for: {table_name}')

@flow(name="Ingest Flow")
def main_flow(table_name: str, url: str):

    file_type, file_name = extract_file_data(url)
    log_subflow(table_name)

    connection_block = SqlAlchemyConnector.load('postgres-connector') 
    with connection_block.get_connection(begin=False) as engine:
    
        if file_type == 'parquet':
            load_parquet_file(file_name, table_name, engine)
        else:
            load_csv_file(file_name, table_name, engine)


if __name__ == '__main__':
    main_flow(table_name='yellow_taxi_trips_bu', url='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet')


