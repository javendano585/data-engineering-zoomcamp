
import os
import argparse

import pandas as pd
import pyarrow.parquet as pq

from sqlalchemy import create_engine
from time import time

def load_parquet_file(file_name, table_name, engine):
    file_initialized = False

    parquet_file = pq.ParquetFile(file_name)
    for batch in parquet_file.iter_batches():
        t_start = time()
        batch_df = (
            batch.to_pandas()
            .astype({
                'passenger_count': 'Int64',
                'RatecodeID': 'Int64',
            })
        )
        if not file_initialized:
            batch_df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
            file_initialized = True
        
        batch_df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()
        print(f'Inserted another chunk of size {batch_df.shape}, took {(t_end - t_start):.3f} seconds.')

def load_csv_file(file_name, table_name, engine):
    df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True: 
        try:
            t_start = time()
            
            df = next(df_iter)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break 

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url

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
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    if file_type == 'parquet':
        load_parquet_file(file_name, table_name, engine)
    else:
        load_csv_file(file_name, table_name, engine)
    
    os.system(f'wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv -O taxi+_zone_lookup.csv')
    load_csv_file('taxi+_zone_lookup.csv', 'zones', engine)
    


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the file')

    args = parser.parse_args()

    main(args)


