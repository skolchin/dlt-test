import os
import dlt
import click
import dotenv
import logging
from dlt.sources.sql_database import sql_database
from typing import Literal, get_args

dotenv.load_dotenv()
dlt.config["runtime.log_level"] = "NONE"

logging.basicConfig(
    format="%(message)s",
    level=logging.INFO,
    force=True)

_logger = logging.getLogger(__name__)

LoadMode = Literal["initial", "incremental"]
LoadStrategy = Literal["append", "merge"]

def make_source(load_mode: LoadMode = "initial", strategy: LoadStrategy = "append"):
    source = sql_database(credentials=dlt.secrets["sources.source_db.credentials"])

    match load_mode:
        case "initial":
            source.dict1_data.apply_hints(
                write_disposition="append",
            )
            source.dict2_data.apply_hints(
                write_disposition="append",
            )
            source.table_data.apply_hints(
                write_disposition="append",
            )

        case "incremental":
            source.dict1_data.apply_hints(
                incremental=dlt.sources.incremental("modified", on_cursor_value_missing="exclude"),
                write_disposition={ "disposition": strategy, "strategy": "upsert" },
            )
            source.dict2_data.apply_hints(
                write_disposition={ "disposition": strategy, "strategy": "scd2" },
            )
            source.table_data.apply_hints(
                incremental=dlt.sources.incremental("modified", on_cursor_value_missing="exclude"),
                write_disposition={ "disposition": strategy, "strategy": "upsert" },
            )

    return source

@dlt.source
def source_initial():
    return make_source("initial", "append")

@dlt.source
def source_incremental():
    return make_source("incremental", "append")

@dlt.source
def source_merge():
    return make_source("incremental", "merge")

def run_pipeline(load_mode: LoadMode, strategy: LoadStrategy):
    """Sample pipeline"""

    p = dlt.pipeline(
        pipeline_name="stage_data_load_test",
        destination="postgres",
        dataset_name="public",
        progress="alive_progress",
        refresh="drop_data" if load_mode == "initial" else None, # type:ignore
    )

    # Run the pipeline
    match load_mode:
        case "initial":
            source = source_initial
            strategy = "append"
        case "incremental" if strategy == "append":
            source = source_incremental
        case "incremental" if strategy == "merge":
            source = source_merge
        case _:
            raise ValueError(load_mode)

    _logger.info(f"Running data load in '{load_mode}:{strategy}' mode")

    _logger.info(
        p.run(
            source,
            credentials=dlt.secrets["destination.stage_db.credentials"],
        )
    )

@click.command
@click.argument("mode", type=click.Choice(get_args(LoadMode)), default="initial")
@click.argument("strategy", type=click.Choice(get_args(LoadStrategy)), default="append")
def main(mode: LoadMode, strategy: LoadStrategy):
    run_pipeline(mode, strategy)

if __name__ == "__main__":
    main()
