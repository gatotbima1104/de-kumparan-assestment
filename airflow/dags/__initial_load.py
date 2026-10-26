from datetime import timedelta

import pendulum
from airflow.sdk import dag
from include.usecases.initial_load import run_initial_load_usecase


@dag(
    dag_id="initial_load",
    description="Initial load articles from PostgreSQL to GCS",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["load", "postgres", "gcs"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
)
def initial_load():
    run_initial_load_usecase()


initial_load()
