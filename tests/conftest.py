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
from lib.data_processing import DataProcessor, TableDefs, TableCounts

dotenv.load_dotenv()

_logger = logging.getLogger()

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

@pytest.fixture(scope="function")
def source_db(source_db_url: URL, postgres) -> Engine:
    """Source database SQLA engine"""
    return create_engine(source_db_url)

@pytest.fixture(scope="function")
def stage_db(stage_db_url: URL, postgres) -> Engine:
    """Target database SQLA engine"""
    return create_engine(stage_db_url)

@pytest.fixture(scope="function")
def source_processor(source_db: Engine) -> DataProcessor:
    """Data processor for source database"""
    return DataProcessor(source_db)

@pytest.fixture(scope="function")
def stage_processor(stage_db: Engine) -> DataProcessor:
    """Data processor for stage database"""
    return DataProcessor(stage_db)

@pytest.fixture(scope="function")
def source_tables(source_processor: DataProcessor) -> TableDefs:
    """All source tables and views"""
    return source_processor.all_tables

@pytest.fixture(scope="function")
def stage_tables(stage_processor: DataProcessor) -> TableDefs:
    """All stage tables and views"""
    return stage_processor.all_tables

@pytest.fixture
def source_counts(source_processor: DataProcessor):
    """Count table records for all source tables (callable fixture)"""
    def _execute() -> TableCounts:
        return source_processor.get_counts()
    return _execute

@pytest.fixture
def source_modified_counts(source_processor: DataProcessor):
    """Count modified table records for all source tables where applicable (callable fixture)"""
    def _execute() -> TableCounts:
        return source_processor.get_modified_counts()
    return _execute

@pytest.fixture
def stage_counts(stage_processor: DataProcessor):
    """Count table records for all stage tables (callable fixture)"""
    def _execute() -> TableCounts:
        return stage_processor.get_counts()
    return _execute

@pytest.fixture
def populate_source(source_processor: DataProcessor):
    """Fill database with test data (callable fixture)"""
    def _execute() -> TableCounts:
        return source_processor.populate()
    return _execute

@pytest.fixture
def modify_source(source_processor: DataProcessor):
    """Modify some source data (callable fixture)"""
    def _execute() -> TableCounts:
        return source_processor.modify()
    return _execute

@pytest.fixture
def clear_stage(stage_processor: DataProcessor):
    """Clear target database (callable fixture)"""
    def _execute():
        return stage_processor.clear()
    return _execute
