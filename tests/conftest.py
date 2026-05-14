import os
import pytest
import dotenv
import logging
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine
from lib.data_processing import DataProcessor

dotenv.load_dotenv()

_logger = logging.getLogger(__name__)

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
