
import logging
from datetime import timedelta

import pendulum
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sdk import task
from include.constant import (
    BQ_RAW_DATASET,
    GCS_BUCKET_NAME,
    OVERLAP_MINUTES,
    TABLE_CONFIGS,
    WINDOW_HOUR,
)
from include.tasks.bigquery import load_gcs_to_bigquery

from .initial_load import create_load_configs, extract_to_gcs, trigger_transform

logger = logging.getLogger(__name__)

@task
def create_hourly_job(data_interval_start=None, data_interval_end=None) -> list[dict]:
    """ [TASK] Create hourly job
            Ex. output
            [
                {
                    "table_name": "articles",
                    "label": "hourly_2026-10-26_10",
                    "start": "2026-10-26T09:50:00Z",
                    "end": "2026-10-26T10:00:00Z"},
                {
                    "table_name": "authors",
                    "label": "snapshot",
                    "start": None,
                    "end": None,
                },
                {
                    "table_name": "categories",
                    "label": "snapshot",
                    "start": None,
                    "end": None,
                },
            ]
    """
    
    if data_interval_start is None or data_interval_end is None:
        data_interval_end = pendulum.now("UTC").start_of("hour")
        data_interval_start = data_interval_end.subtract(hours=WINDOW_HOUR)

    start = data_interval_start - timedelta(minutes=OVERLAP_MINUTES)
    end = data_interval_end
    label = f"hourly_{end.format('YYYY-MM-DD_HH')}"

    jobs = []
    for table_name, config in TABLE_CONFIGS.items():
        if config["strategy"] == "full":
            jobs.append(
                {
                    "table_name": table_name,
                    "label": "snapshot",
                    "start": None,
                    "end": None,
                }
            )
        else:
            jobs.append(
                {
                    "table_name": table_name,
                    "label": label,
                    "start": start.to_iso8601_string(),
                    "end": end.to_iso8601_string(),
                }
            )

    logger.info("Hourly jobs created | count=%d | window=%s to %s", len(jobs), start, end)
    return jobs

@task
def create_merge_configs(extracted: list[dict]) -> list[dict]:
    """ [TASK] Merge changes data in PostgreSQL
            Input
            [
                {
                    "table_name": "articles",
                    "rows": 150,
                    "object_name": "raw/articles/hourly_2026-10-26_10.parquet",
                },
                {
                    "table_name": "authors",
                    "rows": 20,
                    "object_name": "raw/authors/snapshot.parquet",
                },
                {
                    "table_name": "categories",
                    "rows": 0,
                    "object_name": None,
                },
            ]
    """
    
    configs = []
    for result in extracted:
        table_name = result["table_name"]
        config = TABLE_CONFIGS[table_name]

        logger.info("Hourly extraction result | table=%s | rows=%s | object=%s", table_name, result.get("rows", 0), result.get("object_name"))

        # if non monthly, rows=0, None object in GCS
        if (config["strategy"] != "monthly" or not result["rows"] or not result["object_name"]):
            continue

        primary_key = config["primary_key"]
        raw = f"{BQ_RAW_DATASET}.{config['bq_table']}"
        batch = f"{raw}_batch"
        uri = f"gs://{GCS_BUCKET_NAME}/{result['object_name']}"

        sql = f"""
            LOAD DATA OVERWRITE `{batch}`
            FROM FILES (format = 'PARQUET', uris = ['{uri}']);
 
            BEGIN TRANSACTION;
            
            DELETE FROM `{raw}` 
            WHERE {primary_key} IN (
                SELECT {primary_key} FROM `{batch}`
            );
            
            INSERT INTO `{raw}` 
            SELECT * FROM `{batch}`;
            
            COMMIT TRANSACTION;
        """
        configs.append({"query": {"query": sql, "useLegacySql": False}})

    if not configs:
        logger.warning("No merge configs generated | no non-empty incremental extraction results")

    return configs

# workflow
def run_hourly_usecase():
    """ Workflow DAG """
    
    jobs = create_hourly_job()
    extracted = extract_to_gcs.expand(job=jobs)

    dim_configs = create_load_configs(strategy="full")
    load_dims = load_gcs_to_bigquery(dim_configs)

    merge_configs = create_merge_configs(extracted)
    
    merge_articles = BigQueryInsertJobOperator.partial(
        task_id="merge_articles_into_raw",
        gcp_conn_id="google_cloud_default",
    ).expand(
        configuration=merge_configs
    )
    
    trigger_dbt = trigger_transform(source="HOURLY")
    
    # dag dependencies
    [merge_articles, load_dims] >> trigger_dbt
