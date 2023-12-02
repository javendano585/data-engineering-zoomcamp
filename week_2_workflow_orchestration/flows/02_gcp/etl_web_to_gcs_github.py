from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues"""
    df_clean = (
        df
        .assign(
            passenger_count = lambda _df: _df.passenger_count.astype('Int64'),
            RatecodeID = lambda _df: _df.RatecodeID.astype('Int64'),
            )
    )

    if color == 'yellow':
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime),
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    elif color == 'green':
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime),
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

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
        to_path=path.as_posix()
    )

@flow()
def etl_web_to_gcs() -> None:
    """ The main ELT function """
    color = 'yellow'
    year = 2021
    month = 1
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    df_clean = clean(df, color)
    path = write_local(df_clean, color, dataset_file)

    write_gcs(path)



if __name__ == '__main__':
    etl_web_to_gcs()

