
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

from include.constant import (
    GCP_CONN_ID,
    GCS_BUCKET_NAME,
)


def load_gcs_to_bigquery(load_configs):
    """[TASK] Load from GCS to Bigquery"""
    return GCSToBigQueryOperator.partial(
        task_id="load_raw_tables",
        bucket=GCS_BUCKET_NAME,
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
        gcp_conn_id=GCP_CONN_ID,
    ).expand_kwargs(load_configs)