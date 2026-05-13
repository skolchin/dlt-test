import dlt
from airflow.sdk import dag
from dlt.common import pendulum
from dlt.sources.sql_database import sql_database
from dlt.helpers.airflow_helper import PipelineTasksGroup

@dag(
    dag_id="dlt-stage-data-load",
    schedule=None,
    start_date=pendulum.DateTime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 0,
    },
)
def data_load_dag():
    # Setup the source
    source = sql_database(
        credentials=dlt.secrets["sources.source_db.credentials"],
        table_names=["dict1_data", "dict2_data", "table_data"],
    )

    # Modify the pipeline parameters
    pipeline = dlt.pipeline(
        pipeline_name="dlt_stage_load",
        destination="stage_db",
        dataset_name="public",
        dev_mode=False,
        progress="log",
    )

    # Create the source, the "serialize" decompose option
    # will convert dlt resources into Airflow tasks.
    # Use "none" to disable it.
    tasks = PipelineTasksGroup("data-load")
    tasks.add_run(
        pipeline,
        source,
        retries=0,
        decompose="serialize",
        trigger_rule="all_done",
        write_disposition="append",
    )

    # The "parallel" decompose option will convert dlt
    # resources into parallel Airflow tasks. By default the
    # first source component runs before the rest to safely
    # create the initial schema. Set serialize_first_task=False
    # to fan out all components concurrently.
    # All the tasks will be executed in the same pipeline state.
    # tasks.add_run(
    #   pipeline,
    #   source(),
    #   decompose="parallel",
    #   serialize_first_task=False,  # True by default
    #   trigger_rule="all_done",
    #   retries=0,
    #   provide_context=True
    # )

    # The "parallel-isolated" decompose option will convert dlt
    # resources into parallel Airflow tasks, except the
    # first one, which will be executed before any other tasks.
    # In this mode, all the tasks will use separate pipeline states.
    # tasks.add_run(
    #   pipeline,
    #   source(),
    #   decompose="parallel-isolated",
    #   serialize_first_task=False,  # True by default
    #   trigger_rule="all_done",
    #   retries=0,
    #   provide_context=True
    # )

data_load_dag()
