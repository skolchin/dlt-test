import dlt
import pytest
import logging
from sqlalchemy import create_engine
from dlt.sources.sql_database import sql_database

_logger = logging.getLogger()
dlt.config["runtime.log_level"] = "NONE"

@pytest.mark.parametrize("strategy", ["append", "merge"])
def test_stage_load(
        source_tables,
        source_counts,
        stage_counts,
        populate_source,
        modify_source,
        clear_stage,
        strategy):
    """Test initial / incremental source->stage data load"""

    # Setup
    source = sql_database(
        credentials=dlt.secrets["sources.source_db.credentials"],
        table_names=[t.name for t in source_tables.values()])

    populate_source()
    clear_stage()

    p = dlt.pipeline(
        pipeline_name="stage_data_load",
        destination="postgres",
        dataset_name="public",
    )

    # Run the pipeline
    _logger.info("Running initial data load")
    _logger.info(p.run(
        source,
        credentials=dlt.secrets["destination.stage_db.credentials"],
        write_disposition="append"))

    # Check counts
    source_counts_ = source_counts()
    stage_counts_ = stage_counts()
    _logger.info(f"Number of records at source: {source_counts_}")
    _logger.info(f"Number of records at stage: {stage_counts_}")

    for t in source_counts_:
        assert source_counts_[t] == stage_counts_[t], f"Record counts do not match for table {t}"

    # Modify data
    modified_counts = modify_source()

    # Setup incremental load
    source.dict1_data.apply_hints(
        incremental=dlt.sources.incremental("modified", on_cursor_value_missing="exclude"),
        write_disposition=strategy,
    )
    source.dict2_data.apply_hints(
        write_disposition="append",     # no timestamps, always append
    )
    source.table_data.apply_hints(
        incremental=dlt.sources.incremental("modified", on_cursor_value_missing="exclude"),
        write_disposition=strategy,
    )

    # Run once again
    _logger.info("Running incremental data load")
    _logger.info(p.run(
        source,
        credentials=dlt.secrets["destination.stage_db.credentials"],
    ))

    # Check counts
    source_counts_ = source_counts()
    stage_counts_ = stage_counts()
    _logger.info(f"Number of records at source: {source_counts_}")
    _logger.info(f"Number of records at stage: {stage_counts_}")

    stage_counts_a = {k: v for k, v in stage_counts_.items() if k in ["dict1_a", "data_a"]}
    stage_counts_m = {k: v for k, v in stage_counts_.items() if k in ["dict1", "data"]}
    stage_counts_t = {k: v for k, v in stage_counts_.items() if k in ["dict2"]}

    for t in stage_counts_a:
        assert stage_counts_a[t] == source_counts_[t.removesuffix("_a")], f"Record counts do not match for table {t}"

    if strategy == "append":
        for t in stage_counts_m:
            assert stage_counts_m[t] == source_counts_[t] + modified_counts[t], f"Record counts do not match for table {t}"
    else:
        for t in stage_counts_m:
            assert stage_counts_m[t] == source_counts_[t], f"Record counts do not match for table {t}"

    for t in stage_counts_t:
        assert stage_counts_t[t] == 2*source_counts_[t], f"Record counts do not match for table {t}"

def test_stage_load_with_replace(
        source_db,
        stage_db,
        populate_source,
        make_table,
        get_counts):
    """Test source->stage data load with target table replacement"""

    # Setup
    populate_source()

    source_tables = {"dict1_data": make_table(source_db, "dict1_data")}
    source = sql_database(
        credentials=dlt.secrets["sources.source_db.credentials"], 
        table_names=[t.name for t in source_tables.values()])

    with stage_db.connect() as conn:
        conn.exec_driver_sql("drop schema if exists test_replace cascade")

    p = dlt.pipeline(
        pipeline_name="stage_data_load_with_replace",
        destination="postgres",
        dataset_name="test_replace",
    )

    # Run the pipeline
    _logger.info("Running data load twice")
    _logger.info(p.run(
        source,
        credentials=dlt.secrets["destination.stage_db.credentials"],
        write_disposition="replace"))
    _logger.info(p.run(
        source,
        credentials=dlt.secrets["destination.stage_db.credentials"],
        write_disposition="replace"))

    stage_tables = {"dict1_data": make_table(stage_db, "dict1_data", "test_replace")}
    source_counts = get_counts(source_db, source_tables)
    stage_counts = get_counts(stage_db, stage_tables)

    for t in stage_counts:
        assert stage_counts[t] == source_counts[t], f"Record counts do not match for table {t}"

def test_stage_load_custom_source(
        source_db,
        source_db_url,
        stage_db,
        populate_source,
        make_table,
        get_counts):
    """Test source->stage data load using custom selection queries"""

    @dlt.source(name="custom_source")
    def custom_source():

        @dlt.resource(
                name="dict1_data",
                write_disposition="replace",
                columns={"modified": {"data_type": "timestamp"}})
        def dict1_table():
            engine = create_engine(source_db_url)
            with engine.connect() as conn:
                rows = conn.exec_driver_sql("select * from public.dict1_data limit 3")
                for row in rows:
                    yield dict(row._mapping)

        @dlt.resource(name="dict2_data", write_disposition="replace")
        def dict2_table():
            engine = create_engine(source_db_url)
            with engine.connect() as conn:
                rows = conn.exec_driver_sql("select * from public.dict2_data limit 3")
                for row in rows:
                    yield dict(row._mapping)

        return [dict1_table, dict2_table]

    # Setup
    populate_source()

    with stage_db.connect() as conn:
        conn.exec_driver_sql("drop schema if exists test_custom_source cascade")

    p = dlt.pipeline(
        pipeline_name="stage_data_load_custom_source",
        destination="postgres",
        dataset_name="test_custom_source",
    )

    # Run the pipeline
    _logger.info("Running data load")
    _logger.info(p.run(
        custom_source(),
        credentials=dlt.secrets["destination.stage_db.credentials"]))

    source_tables = {t: make_table(source_db, t) for t in ["dict1_data", "dict2_data"]}
    stage_tables = {t: make_table(stage_db, t, "test_custom_source") for t in source_tables}
    stage_counts = get_counts(stage_db, stage_tables)

    for t in stage_counts:
        assert stage_counts[t] == 3, f"Record counts do not match for table {t}"
