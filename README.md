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


## Usage

Several tests are currently implemented, to run all:

```bash
pytest
```
