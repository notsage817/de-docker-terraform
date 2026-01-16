#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine
import click
# import time
# import psycopg2
# import pyarrow.parquet as pq
# import fsspec
# import polars as pl
# Read a sample of the data
# prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
# df = pd.read_csv(prefix + '/yellow_tripdata_2021-01.csv.gz', nrows=100)

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


# Read a sample of the data
def load_data(url, engine, target_table, year: int, month: int, chunksize=100000):

    df_iter = pd.read_csv(
        url + f'/yellow_tripdata_{year:04d}-{month:02d}.csv.gz',
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first = True
    records=0
    for df_chunk in tqdm(df_iter):
        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(
                name=f"{target_table}_{year:04d}{month:02d}",
                con=engine,
                if_exists="replace"
            )
            first = False
            print(f"Table {target_table}_{year:04d}{month:02d} created.")

        # Insert chunk
        df_chunk.to_sql(
            name=f"{target_table}_{year:04d}{month:02d}",
            con=engine,
            if_exists="append"
        )
        records+=len(df_chunk)
        print(f"Inserted: {records} records into {target_table}_{year:04d}{month:02d}.")

# Read Parquet data with polars
# url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet'
# pl_dtype = {
#     "VendorID": "Int64",
#     "passenger_count": "Int64",
#     "trip_distance": "float64",
#     "RatecodeID": "Int64",
#     "store_and_fwd_flag": "string",
#     "PULocationID": "Int64",
#     "DOLocationID": "Int64",
#     "payment_type": "Int64",
#     "fare_amount": "float64",
#     "extra": "float64",
#     "mta_tax": "float64",
#     "tip_amount": "float64",
#     "tolls_amount": "float64",
#     "improvement_surcharge": "float64",
#     "total_amount": "float64",
#     "congestion_surcharge": "float64"
# }
# for k,v in dtype.items():
#     if v == 'Int64':
#         pl_dtype[k] = pl.Int64
#     if v == 'float64':
#         pl_dtype[k] = pl.Float64
#     if v == 'string':
#         pl_dtype[k] = pl.String

# df = pl.read_parquet(url).cast(pl_dtype)

# start = time.time()
# df.write_database(
#     table_name="yellow_taxi_data_202511_pl",
#     connection=db,
#     if_table_exists="replace", # or "append"
#     engine="adbc"              # Faster engine for Postgres
# )
# print(f'{time.time()-start}s spent inserting {len(df)} records.')

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')

def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):
    db = f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    engine = create_engine(db)
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'

    load_data(prefix, engine, target_table, year, month, chunksize)

    engine.dispose()


if __name__ == "__main__":
    main()