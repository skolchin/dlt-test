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
    for t in ["dict1_data", "dict2_data", "table_data"]:
        assert inspector.has_table(t), f"Table '{t}' was not found"

def test_database_data(
        source_counts,
        source_modified_counts,
        stage_counts,
        populate_source,
        clear_stage,
        modify_source):
    """Test database population/update/clearing fixtures"""

    num_recs = populate_source()
    counts = source_counts()
    _logger.info(f"Number of records at source: {counts}")
    for t in counts:
        assert num_recs[t] == counts[t], f"Record counts do not match for table {t}"

    num_recs = modify_source()
    counts = source_modified_counts()
    _logger.info(f"Number of altered records at source: {counts}")
    for t in counts:
        assert num_recs[t] == counts[t], f"Record counts do not match for table {t}"

    clear_stage()
    counts = stage_counts()
    _logger.info(f"Number of records at stage: {counts}")
    for t in counts:
        assert counts[t] == 0, f"Record counts do not match for table {t}"
