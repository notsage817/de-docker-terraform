#!/usr/bin/env python
# coding: utf-8

from tracemalloc import start
import polars as pl
import time
import click
import adbc_driver_postgresql.dbapi as dbapi
import psycopg2

# Read Parquet data with polars
yellow_pl_dtype = {
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
    "congestion_surcharge": "float64",
    "Airport_fee": "float64"
}
green_pl_dtype = {
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
    "congestion_surcharge": "float64",
    "ehail_fee": "float64",
    "trip_type": "Int64"
}

for k,v in yellow_pl_dtype.items():
    if v == 'Int64':
        yellow_pl_dtype[k] = pl.Int64
    if v == 'float64':
        yellow_pl_dtype[k] = pl.Float64
    if v == 'string':
        yellow_pl_dtype[k] = pl.String

for k,v in green_pl_dtype.items():
    if v == 'Int64':
        green_pl_dtype[k] = pl.Int64
    if v == 'float64':
        green_pl_dtype[k] = pl.Float64
    if v == 'string':
        green_pl_dtype[k] = pl.String

def load_data(url, datasource,table_name, db, month, year):
    if datasource == 'yellow_tripdata':
        pl_dtype = yellow_pl_dtype
    elif datasource == 'green_tripdata':
        pl_dtype = green_pl_dtype

    df = pl.read_parquet(f'{url}{datasource}_{year:04d}-{month:02d}.parquet').cast(pl_dtype)
    df.write_database(
        table_name=f"{table_name}_{year:04d}{month:02d}_pl",
        connection=db,
        if_table_exists="replace", # or "append"
        engine="adbc"              # Faster engine for Postgres
    )
    print(f'Inserted: {len(df)} records into {table_name}_{year:04d}{month:02d}_pl.')

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--datasource', default='yellow_tripdata', help='Data source name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, datasource, year, month, target_table):
    db = f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'

    load_data(url, datasource, target_table, db, month, year)

    conn = psycopg2.connect(db)
    conn.set_session(autocommit=True) # THIS IS THE KEY LINE
    cur = conn.cursor()

    cur.execute(f"VACUUM ANALYZE {target_table}_{year:04d}{month:02d}_pl;")

    cur.close()
    conn.close()
    print('maintenance done.')

if __name__ == "__main__":
    main()
