# DLT test project

Experiments with DLT / DBT / Airflow

## Requirements

- Docker (used to run PostgreSQL)
- Python3.14+ (actualy it will work with any recent version)

## Setup

1. Setup environment:

```bash
uv sync
```

2. Copy `.env.sample` to `.env` and fill the `POSTGRES_PASSWORD` variable

3. Copy `.dlt/secrets.toml.sample` to `.dlt/secrets.toml` and fill all the `password` variables with the generated password

4. Make a directory link to dlt config to be used under airflow:

```bash
ln -s ./dlt airflow/dags/.dlt
```

## Usage

Several tests are currently implemented, to run all:

```bash
pytest
```

Airflow is to be started as standalone:

```bash
source setenv.sh
airflow standalone
```

To reset dlt state, remove all pipelines:

```bash
rm -rf ~/.dlt
```

Also, consider truncating `_dlt*` tables in stage database.
