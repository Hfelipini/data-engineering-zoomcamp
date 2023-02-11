from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    print(" 1 ---------------------------------")
    gcs_block = GcsBucket.load("zoom-gcs")
    print(" 2 ---------------------------------")
    gcs_block.get_directory(from_path=gcs_path, local_path=gcs_path)
    print(" 3 ---------------------------------")
    #print(Path(f"../data/{gcs_path}"))
    #return Path(f"../data/{gcs_path}")
    print(Path(f"{gcs_path}"))
    return Path(gcs_path)


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    print("4 ---------------------------------")
    print(path)
    print("5 ---------------------------------")
    df = pd.read_parquet(path)
    print("6 ---------------------------------")
    df.head(10)
    print("7 ---------------------------------")
    #print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    #df["passenger_count"].fillna(0, inplace=True)
    #print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    print("8 ---------------------------------")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.homework-q3",
        project_id="sacred-pursuit-376214",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    color = "yellow"
    year = 2019
    months = [1, 2]
    sizeRows = 0

    for month in months:
        path = extract_from_gcs(color, year, month)
        df = transform(path)
        write_bq(df)
        sizeRows = sizeRows + len(df)
        print("Total number of rows processed:", sizeRows)


if __name__ == "__main__":
    etl_gcs_to_bq()
