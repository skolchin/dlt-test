import os
import random
import pytest
import dotenv
import logging
from time import sleep
from datetime import timedelta
from sqlalchemy.engine import URL, Engine
from sqlalchemy import (
    create_engine,
    select,
    update,
    delete,
    insert,
    bindparam,
    func,
    Table,
    MetaData,
)
from typing import Any, TypedDict, Dict, cast

dotenv.load_dotenv()

_logger = logging.getLogger()

class SourceTables(TypedDict):
    """Source table references"""
    dict1: Table
    dict2: Table
    data: Table

class StageTables(TypedDict):
    """Stage table references"""
    dict1: Table
    dict2: Table
    data: Table
    dict1_a: Table
    dict2_a: Table
    data_a: Table

TableCounts = Dict[str, int]
"""Number of records in tables"""

DEFAULT_NUM_RECS = {
    "data": {
        "generate": 1000,
        "alter": 300,
    },
    "dict1": {
        "generate": 10,
        "alter": 5,
    },
    "dict2": {
        "generate": 10,
        "alter": 5,
    },
}

@pytest.fixture
def make_db_url():
    """SQLA URL (factory fixture)"""
    assert "POSTGRES_USER" in os.environ, f"Postgres creds not set, check .env"

    def _create(db_name: str) -> URL:
        url= URL.create(
            drivername="postgresql+psycopg2",
            username=os.environ["POSTGRES_USER"],
            password=os.environ.get("POSTGRES_PASSWORD"),
            host="localhost",
            port=int(os.environ.get("POSTGRES_PORT", 15432)),
            database=db_name,
        )
        return url

    return _create

@pytest.fixture(scope="session")
def postgres():
    """Start Postgres instance"""
    import subprocess

    p = subprocess.run(["docker", "ps", "-q", "-f", "name=dlt-postgres"], check=True, capture_output=True)
    if not p.stdout:
        _logger.info("Starting database")
        subprocess.run(["bash", "./start_postgres.sh"], check=True)
        sleep(5)

    yield

@pytest.fixture
def source_db_url(make_db_url) -> URL:
    """Source database SQLA URL"""
    return make_db_url("source_db")

@pytest.fixture
def stage_db_url(make_db_url) -> URL:
    """Target database SQLA URL"""
    return make_db_url("stage_db")

@pytest.fixture
def source_db(source_db_url, postgres) -> Engine:
    """Source database SQLA engine"""
    return create_engine(source_db_url)

@pytest.fixture
def stage_db(stage_db_url, postgres) -> Engine:
    """Target database SQLA engine"""
    return create_engine(stage_db_url)

@pytest.fixture
def make_table():
    """SQLA Table factory fixture"""
    def _create(db: Engine, table_name: str, schema_name: str = "public") -> Table:
        meta = MetaData(schema=schema_name)
        table = Table(table_name, meta, autoload_with=db)
        return table
    return _create

@pytest.fixture
def source_tables(source_db: Engine, make_table) -> SourceTables:
    """All source tables"""
    return SourceTables(
        dict1=make_table(source_db, "dict1_data"),
        dict2=make_table(source_db, "dict2_data"),
        data=make_table(source_db, "table_data"),
    )

@pytest.fixture
def stage_tables(stage_db: Engine, make_table) -> StageTables:
    """All stage tables and views"""
    return StageTables(
        dict1=make_table(stage_db, "dict1_data"),
        dict2=make_table(stage_db, "dict2_data"),
        data=make_table(stage_db, "table_data"),
        dict1_a=make_table(stage_db, "dict1_data_a"),
        dict2_a=make_table(stage_db, "dict2_data_a"),
        data_a=make_table(stage_db, "table_data_a"),
    )

@pytest.fixture
def get_counts():
    """Count table records (factory fixture)"""
    def _execute(db: Engine, tables: Dict[str, Table]) -> TableCounts:
        with db.connect() as conn:
            counts = {t: conn.scalar(select(func.count()).select_from(tables[t])) for t in tables}
            return cast(TableCounts, counts)
    return _execute

@pytest.fixture
def get_modified_counts():
    """Count modified table records (factory fixture)"""
    def _execute(db: Engine, tables: Dict[str, Table]) -> TableCounts:
        with db.connect() as conn:
            table_names = set(tables.keys()).intersection(["dict1", "data"])
            counts = {
                t: conn.scalar(select(func.count()).select_from(tables[t]).where(tables[t].c.modified.isnot(None)))
                for t in table_names}
            return cast(TableCounts, counts)
    return _execute

@pytest.fixture
def source_counts(source_db: Engine, source_tables: SourceTables, get_counts):
    """Count table records for all source tables (callable fixture)"""
    def _execute() -> TableCounts:
        return get_counts(source_db, source_tables)
    return _execute

@pytest.fixture
def source_modified_counts(source_db: Engine, source_tables: SourceTables, get_modified_counts):
    """Count modified table records for all source tables where applicable (callable fixture)"""
    def _execute() -> TableCounts:
        return get_modified_counts(source_db, source_tables)
    return _execute

@pytest.fixture
def stage_counts(stage_db: Engine, stage_tables: StageTables, get_counts):
    """Count table records for all stage tables (callable fixture)"""
    def _execute() -> TableCounts:
        return get_counts(stage_db, stage_tables)
    return _execute

def randstr(n: int) -> str:
    return "".join([chr(random.randint(ord("A"), ord("z"))) for _ in range(n)])

def valuestr(n: int) -> str:
    return f"Value {n+1}"

@pytest.fixture
def populate_source(source_db: Engine, source_tables: SourceTables):
    """Fill database with test data (callable fixture)"""

    def _execute() -> TableCounts:
        data_table = source_tables["data"]
        dict1_table = source_tables["dict1"]
        dict2_table = source_tables["dict2"]

        num_recs = dict(
            data=DEFAULT_NUM_RECS["data"]["generate"],
            dict1=DEFAULT_NUM_RECS["dict1"]["generate"],
            dict2=DEFAULT_NUM_RECS["dict2"]["generate"],
        )
        _logger.info(f"Generating records on source: {num_recs}")

        with source_db.begin() as conn:
            conn.execute(delete(data_table))
            conn.execute(delete(dict1_table))
            conn.execute(delete(dict2_table))

        with source_db.begin() as conn:
            conn.execute(
                insert(dict1_table).values({
                    "dict_value": bindparam("_value"),
                    "created": func.now() - timedelta(days=1),
                }),
                [{"_value": valuestr(n)} for n in range(num_recs["dict1"])])
            
            conn.execute(
                insert(dict2_table),
                [{
                    "dict_value": valuestr(n),
                } for n in range(num_recs["dict2"])])

            dict1_ids = conn.execute(select(dict1_table.c.dict_id)).scalars().all()
            dict2_ids = conn.execute(select(dict2_table.c.dict_id)).scalars().all()

            conn.execute(
                insert(data_table),
                [{
                    "dict1_id": random.choice(dict1_ids),
                    "dict2_id": random.choice(dict2_ids),
                    "comments": randstr(255)
                } for _ in range(num_recs["data"])])
            
        return num_recs

    return _execute

@pytest.fixture
def modify_source(source_db: Engine, source_tables: SourceTables):
    """Modify some source data (callable fixture)"""

    def _execute() -> TableCounts:
        data_table = source_tables["data"]
        dict1_table = source_tables["dict1"]
        dict2_table = source_tables["dict2"]

        num_recs = dict(
            data=DEFAULT_NUM_RECS["data"]["alter"],
            dict1=DEFAULT_NUM_RECS["dict1"]["alter"],
            dict2=DEFAULT_NUM_RECS["dict2"]["alter"],
        )

        with source_db.begin() as conn:
            
            ids = {t: conn.execute(select(source_tables[t].c.id)).scalars().all() for t in ["data"]} | \
                  {t: conn.execute(select(source_tables[t].c.dict_id)).scalars().all() for t in ["dict1", "dict2"]}
            change_ids = {t: random.sample(ids[t], min(num_recs[t], len(ids[t]))) for t in ids}
            change_recs = {t: len(change_ids[t]) for t in change_ids}
            _logger.info(f"Altering records on source: {change_recs}")

            conn.execute(
                update(dict1_table) \
                .where(dict1_table.c.dict_id == bindparam("_id")) \
                .values({
                    "dict_value": bindparam("_value"),
                    "modified": func.now(),
                }),
                [{
                    "_id": i,
                    "_value": randstr(30),
                    "modified": func.now(),
                } for i in change_ids["dict1"]])

            conn.execute(
                update(dict2_table) \
                .where(dict2_table.c.dict_id == bindparam("_id")) \
                .values({
                    "dict_value": bindparam("_value"),
                }),
                [{
                    "_id": i,
                    "_value": randstr(30),
                } for i in change_ids["dict2"]])

            conn.execute(
                update(data_table) \
                .where(data_table.c.id == bindparam("_id")) \
                .values({
                    "dict1_id": bindparam("_dict1_id"),
                    "dict2_id": bindparam("_dict2_id"),
                    "comments": bindparam("_comments"),
                    "modified": func.now(),
                }),
                [{
                    "_id": i,
                    "_dict1_id": random.choice(ids["dict1"]),
                    "_dict2_id": random.choice(ids["dict2"]),
                    "_comments": randstr(255)
                } for i in change_ids["data"]])

        return cast(TableCounts, change_recs)

    return _execute

@pytest.fixture
def clear_stage(stage_db: Engine, stage_tables: StageTables):
    """Clear target database (callable fixture)"""
    def _execute() -> None:
        with stage_db.begin() as conn:
            conn.execute(delete(stage_tables["data"]))
            conn.execute(delete(stage_tables["dict1"]))
            conn.execute(delete(stage_tables["dict2"]))
    return _execute
