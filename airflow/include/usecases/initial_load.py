
import logging
import uuid

import pandas as pd
import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable, task
from include.constant import BQ_RAW_DATASET, GCS_PREFIX, START_YEAR, TABLE_CONFIGS
from include.tasks.bigquery import load_gcs_to_bigquery
from include.tasks.gcs import upload_parquet
from include.tasks.postgres import read_table

logger = logging.getLogger(__name__)


@task
def create_jobs() -> list[dict]:
    """ [TASK] Create jobs for extraction
        Ex. output
        [
            { 
                "table_name": "articles", "label": "2016-01", "start": "2016-01-01T00:00:00Z", "end": "2016-02-01T00:00:00Z"
            },
            {
                "table_name": "articles", "label": "2016-02", "start": "2016-02-01T00:00:00Z", "end": "2016-03-01T00:00:00Z"
            },
            { 
                "table_name": "authors", "label": "snapshot", "start": None, "end": None
            },
        ]
    """
    
    end = pendulum.now("UTC")
    Variable.set("initial_load_started_at", end.to_iso8601_string()) # set var inside airflow xcom

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
            continue

        # monthly-mode
        current = pendulum.datetime(START_YEAR, 1, 1, tz="UTC") # start range_time (START_YEAR,1,1)
        while current < end:
            next_month = current.add(months=1) # next month of current (START_YEAR,2,1)
            jobs.append(
                {
                    "table_name": table_name,
                    "label": current.format("YYYY-MM"),
                    "start": (
                        None
                        if current.year == START_YEAR and current.month == 1 # set to None if same as START_YEAR and first monht
                        else current.to_iso8601_string() # START_YEAR-02-01, etc.
                    ),
                    "end": min(next_month, end).to_iso8601_string() # START_YEAR-03-01, etc.
                }
            )

            current = next_month

    logger.info("Initial load jobs created | count=%d | tables=%s | cutoff=%s", len(jobs), list(TABLE_CONFIGS.keys()), end.to_iso8601_string())
    return jobs

@task(max_active_tis_per_dag=4)
def extract_to_gcs(job: dict) -> dict:
    """ [TASK] Extract from postgres to GCS
            input
            {
                "table_name": "articles", "label": "2026-01", "start": "2026-01-01T00:00:00Z", "end": "2026-02-01T00:00:00Z"
            },
            
            output
            {
                "table_name": "articles", "rows": 150, "object_name": "raw/articles/2026-01.parquet"
            }
    """
    
    table_name = job["table_name"]
    if table_name not in TABLE_CONFIGS:
        raise ValueError(f"Unknown table: {table_name}")

    config = TABLE_CONFIGS[table_name]
    logger.info("Extraction started | table=%s | strategy=%s | start=%s | end=%s", table_name, config["strategy"], job["start"], job["end"])

    # if strategy chunk montly
    if config["strategy"] == "monthly":
        df = read_table(
            table_name=config["pg_table"],
            watermark_col=config["watermark_col"],
            start=job["start"],
            end=job["end"],
        )
    else:
        df = read_table(table_name=config["pg_table"])

    # apply strict type conversions
    for col in config["string_cols"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .map(lambda value: None if pd.isna(value) else str(value))
                .astype("string")
            )

    for col in df.columns:
        if df[col].dtype == "object":
            has_uuid = df[col].map(lambda value: isinstance(value, uuid.UUID)).any()

            if has_uuid:
                df[col] = df[col].map(
                    lambda value: str(value) if isinstance(value, uuid.UUID) else value
                )

    for col in config["timestamp_cols"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True)

    for col in config["integer_cols"]:
        if col in df.columns:
            df[col] = df[col].astype("Int64")

    # skip to create unnecessary empty monthly files
    if df.empty and config["strategy"] == "monthly":
        logger.info("Extraction returned no rows | table=%s | label=%s", table_name, job["label"])
        return {
            "table_name": table_name,
            "rows": 0,
            "object_name": None,
        }

    # upload to gcs
    object_name = upload_parquet(
        df=df,
        table_name=table_name,
        label=job["label"],
    )

    logger.info("Extraction completed | table=%s | rows=%d | gcs_object=%s", table_name, len(df), object_name)
    return {
        "table_name": table_name,
        "rows": len(df),
        "object_name": object_name,
    }

@task
def create_load_configs(strategy: str | None = None) -> list[dict]:
    """ [TASK] Create configs
        output
        [
            {
                "source_objects": ["raw/articles/*.parquet"], 
                "destination_project_dataset_table": "my_project.raw_dataset.articles",
            }
        ]
    """

    configs = []
    for table_name, config in TABLE_CONFIGS.items():
        if strategy is not None and config["strategy"] != strategy: # if params: skip strategy that doesn't defined in params
            continue

        configs.append({
            "source_objects": [f"{GCS_PREFIX}/{table_name}/*.parquet"],
            "destination_project_dataset_table": (
                f"{BQ_RAW_DATASET}.{config['bq_table']}"
            ),
        })

    logger.info(
        "BigQuery load configs created | strategy=%s | count=%d | destinations=%s", strategy, 
        len(configs), [item["destination_project_dataset_table"] for item in configs]
    )

    return configs

def trigger_transform(source: str = "HOURLY"):
    """Trigger dbt transformation"""
    return TriggerDagRunOperator(
        task_id="trigger_transform_article",
        trigger_dag_id="transform_article",
        conf={
            "trigger_source": source
        },
        wait_for_completion=False,
    )

def run_initial_load_usecase():
    """ Workflow DAG """
    
    # cleaned = clean_initial_load()
    jobs = create_jobs()
    extracted = extract_to_gcs.expand(job=jobs)
    
    load_configs = create_load_configs()
    load_to_raw_dwh = load_gcs_to_bigquery(load_configs)
    
    trigger_dbt = trigger_transform(source="INITIAL_LOAD")

    # dag dependencies
    # cleaned >> extracted
    extracted >> load_to_raw_dwh >> trigger_dbt
