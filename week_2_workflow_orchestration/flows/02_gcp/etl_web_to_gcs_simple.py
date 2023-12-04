from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues"""
    print(f'column: {df.dtypes}')

    if color == 'fhv':
        df_clean = (
            df
            .astype(
                {'SR_Flag': 'Int64',
                 'PUlocationID': 'Int64',
                 'DOlocationID': 'Int64'}
            )
        )
    else:
        df_clean = (
            df
            .astype(
                {'passenger_count': 'Int64',
                'RatecodeID': 'Int64',
                'Payment_type': 'Int64'}
            )
        )

    if color == 'yellow':
        df_clean.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df_clean.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    elif color == 'green':
        df_clean.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df_clean.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    elif color == 'fhv':
        df_clean.pickup_datetime = pd.to_datetime(df.pickup_datetime)
        df_clean.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)

    print(df_clean.head(2))
    print(f'column: {df_clean.dtypes}')
    print(f'rows: {len(df_clean)}')
    return df_clean


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as a parquet file"""
    path = Path(f'data/{color}/{dataset_file}.parquet')
    # path = Path('data') / color / f'{dataset_file}.parquet'
    df.to_parquet(path, compression='gzip')
    return path


@task()
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to GCS"""
    
    gcs_block = GcsBucket.load('dezoomcamp-gcs')
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path.as_posix(),
        timeout=120
    )

@flow()
def etl_web_to_gcs() -> None:
    """ The main ELT function """
    color = 'fhv'
    year = 2019
    months = list(range(2, 13))
    for month in months:
        dataset_file = f'{color}_tripdata_{year}-{month:02}'
        dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

        df = fetch(dataset_url)
        df_clean = clean(df, color)
        path = write_local(df_clean, color, dataset_file)

        write_gcs(path)



if __name__ == '__main__':
    etl_web_to_gcs()

