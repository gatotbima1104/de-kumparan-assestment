from src.constants import BQ_DATASET_RAW, BUCKET_NAME, PROJECT_ID, REGION

# connection
POSTGRES_CONN_ID = "article_postgres"
GCP_CONN_ID = "google_cloud_default"

# cloud configs
GCS_BUCKET_NAME = BUCKET_NAME
GCP_PROJECT_ID = PROJECT_ID
GCP_REGION = REGION
BQ_RAW_DATASET = BQ_DATASET_RAW

# setup
START_YEAR = 2016
GCS_PREFIX = "initial_load"
DBT_PROJECT_DIR = "/opt/airflow/project/dbt_article"
OVERLAP_MINUTES = 10
WINDOW_HOUR = 1

TABLE_CONFIGS = {
    "articles": {
        "pg_table": "articles",
        "bq_table": "articles",
        "strategy": "monthly",
        "watermark_col": "updated_at",
        "primary_key": "id",
        "string_cols": ["id", "author_id", "status"],
        "timestamp_cols": [
            "published_at",
            "created_at",
            "updated_at",
            "deleted_at",
        ],
        "integer_cols": ["category_id"],
    },
    "authors": {
        "pg_table": "authors",
        "bq_table": "authors",
        "strategy": "full",
        "primary_key": "id",
        "string_cols": [],
        "timestamp_cols": [],
        "integer_cols": [],
    },
    "categories": {
        "pg_table": "categories",
        "bq_table": "categories",
        "strategy": "full",
        "primary_key": "id",
        "string_cols": [],
        "timestamp_cols": [],
        "integer_cols": [],
    },
}