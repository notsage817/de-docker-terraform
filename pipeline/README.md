This is a basic project that read New York taxi trips data and load them into PostgreSQL.

For CSV file, running the command below with input will load data thru SQLalchemy:

```uv run python ingest_data_pd.py \
  --pg-user=root \
  --pg-pass=root \
  --pg-host=localhost \
  --pg-port=5432 \
  --pg-db=ny_taxi \
  --target-table=yellow_taxi_trips \
  --year=2021 \
  --month=1 \
  --chunksize=100000



For parquet file, running command below with input will load data thru a 10x faster engine 'adbc':

```uv run python ingest_data_pq.py \
  --pg-user=root \
  --pg-pass=root \
  --pg-host=localhost \
  --pg-port=5432 \
  --pg-db=ny_taxi \
  --target-table=yellow_taxi_trips \
  --year=2021 \
  --month=1