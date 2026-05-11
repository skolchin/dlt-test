import dlt
import logging
from sqlalchemy import func, select
from dlt.sources.sql_database import sql_database

_logger = logging.getLogger()
dlt.config["runtime.log_level"] = "NONE"

def test_native_stage_load(
        source_db,
        stage_db,
        source_table,
        source_types,
        stage_table,
        stage_types,
        stage_table_a,
        stage_types_a,
        populate_source,
        modify_source,
        clear_target):
    """Test initial / incremental source->stage data load"""

    # Setup
    source = sql_database(credentials=dlt.secrets["sources.source_db.credentials"])
    populate_source()
    clear_target()

    # Run the pipeline
    _logger.info("Running initial data load")
    p = dlt.pipeline(
        pipeline_name="stage_data_load",
        pipelines_dir="./.pipelines",
        destination="postgres",
        dataset_name="public",
    )
    p.run(
        source,
        credentials=dlt.secrets["destination.stage_db.credentials"],
        write_disposition="append")

    # Check counts
    def get_counts(db, table, types):
        counts = {}
        with db.connect() as conn:
            counts["types"] = conn.scalar(select(func.count()).select_from(types))
            counts["table_data"] = conn.scalar(select(func.count()).select_from(table))
        return counts

    source_counts = get_counts(source_db, source_table, source_types)
    stage_counts = get_counts(stage_db, stage_table, stage_types)
    _logger.info(f"Number of records at source: {source_counts}")
    _logger.info(f"Number of records at stage: {stage_counts}")

    assert source_counts["types"] == stage_counts["types"]
    assert source_counts["table_data"] == stage_counts["table_data"]

    # Modify data
    modified_count = modify_source()

    # Setup incremental load
    source.types.apply_hints(
        primary_key="type_id",
        write_disposition={"disposition": "merge", "strategy": "scd2"},
    )    
    source.table_data.apply_hints(
        incremental=dlt.sources.incremental("modified", on_cursor_value_missing="exclude"),
        write_disposition="append",
    )

    # Run once again
    _logger.info("Running incremental data load")
    p.run(
        source,
        credentials=dlt.secrets["destination.stage_db.credentials"])

    # Check counts
    source_counts = get_counts(source_db, source_table, source_types)
    stage_counts = get_counts(stage_db, stage_table, stage_types)
    stage_a_counts = get_counts(stage_db, stage_table_a, stage_types_a)

    _logger.info(f"Number of records at source: {source_counts}")
    _logger.info(f"Number of records at stage (real / actual): {stage_counts} / {stage_a_counts}")

    assert 2 * source_counts["types"] == stage_counts["types"]
    assert source_counts["table_data"] + modified_count == stage_counts["table_data"]

    assert source_counts["types"] == stage_a_counts["types"]
    assert source_counts["table_data"] == stage_a_counts["table_data"]
