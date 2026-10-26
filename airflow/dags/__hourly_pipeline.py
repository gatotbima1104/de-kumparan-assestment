from datetime import timedelta

import pendulum
from airflow.sdk import dag
from include.usecases.hourly_incremental import run_hourly_usecase


@dag(
    dag_id="hourly_incremental",
    description="Hourly ETL from PosgreSQL to BigQuery",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    schedule="0 * * * *",
    tags=["etl", "postgres", "bigquery"],
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def hourly_incremental():
    run_hourly_usecase()


hourly_incremental()
