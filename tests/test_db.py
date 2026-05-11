import pytest
import logging
from sqlalchemy import inspect, func, select
from pytest_lazy_fixtures.lazy_fixture import lf

_logger = logging.getLogger()

def test_database_urls(source_db_url, stage_db_url):
    """Test database URLS are correct"""
    _logger.info(f"Source database url: {source_db_url.render_as_string()}")
    _logger.info(f"Target database url: {stage_db_url.render_as_string()}")

@pytest.mark.parametrize("db", [lf("source_db"), lf("stage_db")])
def test_database_tables(db):
    """Test database was properly created"""
    inspector = inspect(db)
    for t in ["types", "table_data"]:
        assert inspector.has_table(t), f"Table '{t}' was not found"

@pytest.mark.parametrize(
        ["populate_source", "modify_source"],
        [
            [1000, 300],
            [10000, 3000],
        ],
        ids=["small", "medium"],
        indirect=True
)
def test_database_data(source_db, source_table, stage_db, stage_table, populate_source, clear_target, modify_source):
    """Test database population/update/clearing fixtures"""

    with source_db.connect() as conn:
        num_recs = populate_source()
        count = conn.scalar(select(func.count()).select_from(source_table))
        _logger.info(f"Initial number of records at source: {count}")
        assert num_recs == count

    with source_db.connect() as conn:
        num_recs = modify_source()
        count = conn.scalar(select(func.count()).select_from(source_table).where(source_table.c.modified.is_not(None)))
        _logger.info(f"Number of altered records at source: {count}")
        assert num_recs == count

    with stage_db.connect() as conn:
        clear_target()
        count = conn.scalar(select(func.count()).select_from(stage_table))
        _logger.info(f"Number of records at stage: {count}")
        assert count == 0
