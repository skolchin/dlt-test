import os
import logging
import pytest
import dotenv
import random
from time import sleep
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

dotenv.load_dotenv()

_logger = logging.getLogger()

@pytest.fixture
def make_db_url():
    """SQLA URL factory fixture"""
    assert "POSTGRES_USER" in os.environ, f"Postgres creds not set, check .env"

    def _create(db_name) -> URL:
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
def make_table():
    """SQLA Table factory fixture"""
    def _create(db: Engine, table_name: str, schema_name: str = "public") -> Table:
        meta = MetaData(schema=schema_name)
        table = Table(table_name, meta, autoload_with=db)
        return table
    return _create

@pytest.fixture
def source_table(source_db: Engine, make_table) -> Table:
    """Source data table"""
    return make_table(source_db, "table_data")

@pytest.fixture
def stage_table(stage_db: Engine, make_table) -> Table:
    """Data table at stage"""
    return make_table(stage_db, "table_data")

@pytest.fixture
def stage_table_a(stage_db: Engine, make_table) -> Table:
    """Actual data view for data table at stage"""
    return make_table(stage_db, "table_data_a")

@pytest.fixture
def source_types(source_db: Engine, make_table) -> Table:
    """Source types dictionary"""
    return make_table(source_db, "types")

@pytest.fixture
def stage_types(stage_db: Engine, make_table) -> Table:
    """Types dictionary at stage"""
    return make_table(stage_db, "types")

@pytest.fixture
def stage_types_a(stage_db: Engine, make_table) -> Table:
    """Actual data view for types dictionary at stage"""
    return make_table(stage_db, "types_a")

def randstr(n: int) -> str:
    return "".join([chr(random.randint(ord("A"), ord("z"))) for _ in range(n)])

@pytest.fixture
def populate_source(source_db: Engine, source_table: Table, request):
    """Fill database with test data (factory fixture)"""
    def _execute() -> int:
        num_records = 1000
        if (param := getattr(request, "param", None)):
            num_records = int(param)
        _logger.info(f"Generating {num_records} records on source")

        with source_db.begin() as conn:
            conn.execute(delete(source_table))
            conn.execute(insert(source_table),
                         [{"type_id": random.randint(1,3), "comments": randstr(255)} for _ in range(num_records)])
        return num_records

    return _execute

@pytest.fixture
def clear_target(stage_db: Engine, stage_table: Table, stage_types: Table):
    """Clear target database (factory fixture)"""
    def _execute() -> None:
        with stage_db.begin() as conn:
            conn.execute(delete(stage_table))
            conn.execute(delete(stage_types))
    return _execute

@pytest.fixture
def modify_source(source_db: Engine, source_table: Table, request):
    """Modify some source data (factory fixture)"""
    def _execute() -> int:
        num_records = 300
        if (param := getattr(request, "param", None)):
            num_records = int(param)

        with source_db.begin() as conn:
            stmt = select(source_table.c.id)
            ids = conn.execute(stmt).scalars().all()
            change_ids = random.sample(ids, min(num_records, len(ids)))
            _logger.info(f"Altering {len(change_ids)} out of {len(ids)} records on source")

            conn.execute(
                update(source_table) \
                .where(source_table.c.id == bindparam("_id")) \
                .values({
                    "type_id": bindparam("_type_id"),
                    "comments": bindparam("_comments"),
                    "modified": func.now(),
                }),
                [{"_id": i, "_type_id": random.randint(1,3), "_comments": randstr(255)} for i in change_ids])

        return len(change_ids)

    return _execute
