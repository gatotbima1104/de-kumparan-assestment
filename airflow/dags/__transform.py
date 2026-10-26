from datetime import timedelta

import pendulum
from airflow.sdk import dag
from include.usecases.transform import transform_article_usecase


@dag(
    dag_id="transform_article",
    description="Transform articles with dbt and validate data quality",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["dbt", "transform"],
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)}
)
def transform_article_dag():
    transform_article_usecase()


transform_article_dag()
