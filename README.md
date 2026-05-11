# DLT test project

Experiments with DLT / DBT / Airflow

## Requirements

- Docker (used to run PostgreSQL)
- Python3.14+ (actualy this will work for any supported version, but I prefer latest one)

## Setup

1. Setup environment:

```bash
uv sync
```

2. Copy *.env.sample* to *.env* and fill the *POSTGRES_PASSWORD* variable

3. Copy *.dlt/secrets.toml.sample* to *.dlt/secrets.toml* and fill the *password* variables with the same value (twice)


## Usage

Several tests are currently implemented, to run all:

```bash
pytest
```
