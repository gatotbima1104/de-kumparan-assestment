import io

import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from include.constant import (
    GCP_CONN_ID,
    GCS_BUCKET_NAME,
    GCS_PREFIX,
)


def upload_parquet(
    df: pd.DataFrame,
    table_name: str,
    label: str,
) -> str:
    """Convert datafrom to parquet, upload -> GCS"""
    
    hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    buffer = io.BytesIO()
    df.to_parquet(
        buffer,
        index=False,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )

    object_name = f"{GCS_PREFIX}/{table_name}/{table_name}-{label}.parquet"

    hook.upload(
        bucket_name=GCS_BUCKET_NAME,
        object_name=object_name,
        data=buffer.getvalue(),
        mime_type="application/octet-stream",
    )

    return object_name
