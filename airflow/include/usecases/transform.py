
from include.tasks.dbt import (
    build_intermediate,
    build_marts,
    build_staging,
    snapshot_authors,
    validate_metrics,
)
from include.tasks.quality_check import create_quality_group

from src.constants import (
    BQ_DATASET_INTERMEDIATE,
    BQ_DATASET_MART,
    BQ_DATASET_STAGING,
)


# transoform articles with dbt that will quality checked each layer
def transform_article_usecase() -> None:
    author_snap = snapshot_authors()
    staging = build_staging()

    qa_stg = create_quality_group(
        group_id="quality_check_stg",
        dataset=BQ_DATASET_STAGING,
        table="stg_articles",
        not_null_columns=[
            "id",
            "title",
            "created_at",
        ],
        invalid_rules={
            "empty_title": "TRIM(title) = ''",
        },
    )

    intermediate = build_intermediate()

    qa_int = create_quality_group(
        group_id="quality_check_int",
        dataset=BQ_DATASET_INTERMEDIATE,
        table="int_article_enriched",
        not_null_columns=[
            "article_id",
            "title",
        ],
        invalid_rules={
            "empty_title": "TRIM(title) = ''",
        },
    )

    marts = build_marts()

    qa_mart = create_quality_group(
        group_id="quality_check_mart",
        dataset=BQ_DATASET_MART,
        table="mart_article_daily"
    )
    
    metrics_val = validate_metrics()

    author_snap >> staging >> qa_stg >> intermediate
    intermediate >> qa_int >> marts
    marts >> qa_mart >> metrics_val
