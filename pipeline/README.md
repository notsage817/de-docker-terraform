This is a basic project that read New York taxi trips data and load them into PostgreSQL.

#### Create an virtual environment using uv:
```bash
uv init --python=3.3
```

#### Containizer the pipeline with virtual environment setup
```dockerfile
# Start with slim Python 3.13 image
FROM python:3.13.10-slim

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set working directory
WORKDIR /app

# Add virtual environment to PATH so we can use installed packages
ENV PATH="/app/.venv/bin:$PATH"

# Copy dependency files first (better layer caching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./
# Install dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked

# Copy application code
COPY pipeline.py pipeline.py

# Set entry point
ENTRYPOINT ["python", "pipeline.py"]
```

#### Running postgreSQL in a container
A named volume is created for database. Volume is managed by docker and is persistant regardless of the container.
```bash
docker run -it --rm \
  -e POSTGRES_USER={username} \
  -e POSTGRES_PASSWORD={pw} \
  -e POSTGRES_DB={dbname} \
  -v ny_taxi_postgres_data:/var/lib/postgresql \
  -p 5432:5432 \
  postgres:18
  ```
Or it is known to have a mount from local directory to a docker host, which is not recommended in data engineering where large tables are transformed but welcomed in common DS/ML projects.

#### Run SQL database in terminal
```bash
uv run pgcli -h localhost -p 5432 -u root -d ny_taxi
```

#### Load and ingest data from variable sources
Pipelines in scripts with input to run in terminal.
Click is a useful package for read input.

For CSV file, running the command below with input will load data thru SQLalchemy:

```bash
uv run python ingest_data_pd.py \
  --pg-user=root \
  --pg-pass=root \
  --pg-host=localhost \
  --pg-port=5432 \
  --pg-db=ny_taxi \
  --target-table=yellow_taxi_trips \
  --year=2021 \
  --month=1 \
  --chunksize=100000
```

For parquet file, running command below with input will load data thru a 10x faster engine 'adbc':

```bash
uv run python ingest_data_pq.py \
  --pg-user=root \
  --pg-pass=root \
  --pg-host=localhost \
  --pg-port=5432 \
  --pg-db=ny_taxi \
  --target-table=yellow_taxi_trips \
  --year=2021 \
  --month=1
```

- Engine in SQLalchemy is commonly used especially for Pandas DataFrame, but it is not ideal and also requires loading by chunks of a file.
- Other files requires different dataframe such as Polars, which is more capable of handling large tables and allows 10X faster loading with 'adbc' engine.

#### Manage Database with pgAdmin
Create a new container for pgAdmin and a network for pgAdmin to get access to postgreSQL database.
```bash
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -v pgadmin_data:/var/lib/pgadmin \
  -p 8085:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4
```

#### Dockenize the pipeline
See dockerfile.
```bash
docker build -t taxi_ingest:v001 .
```

```bash
docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --target-table=yellow_taxi_trips \
    --year=2025 \
    --month=11
```


#### Upgrade to docker compose
Docker-compose build containers needed in the whole project in one line and create default volume and network for the project. 
See the docker-compose.yaml file for further details.

```
docker run -it \
  --network=ingestion_newyork_taxi_nw \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --target-table=yellow_taxi_trips \
    --year=2025 \
    --month=10
```
