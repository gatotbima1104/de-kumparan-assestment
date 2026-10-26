from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.sdk import TaskGroup

from include.constant import GCP_CONN_ID, PROJECT_ID


def _create(*, task_id: str, sql: str):
    return BigQueryCheckOperator(
        task_id=task_id, sql=sql, use_legacy_sql=False, gcp_conn_id=GCP_CONN_ID
    )


def check_table_exists(*, task_id: str, dataset: str, table: str):
    return _create(
        task_id=task_id,
        sql=f"""
            SELECT COUNT(*)=1
            FROM `{PROJECT_ID}.{dataset}.INFORMATION_SCHEMA.TABLES`
            WHERE table_name = '{table}'
        """,
    )


def check_row_count(
    *,
    task_id: str,
    dataset: str,
    table: str,
):
    return _create(
        task_id=task_id,
        sql=f"""
            SELECT COUNT(*) > 0
            FROM `{PROJECT_ID}.{dataset}.{table}`
        """,
    )


def check_not_null(
    *, task_id: str, dataset: str, table: str, column: str = "created_at"
):
    return _create(
        task_id=task_id,
        sql=f"""
            SELECT COUNT(*)=0
            FROM `{PROJECT_ID}.{dataset}.{table}`
            WHERE {column} IS NULL
        """,
    )


def check_invalid_value(*, task_id: str, dataset: str, table: str, expression: str):
    return _create(
        task_id=task_id,
        sql=f"""
            SELECT COUNT(*)=0
            FROM `{PROJECT_ID}.{dataset}.{table}`
            WHERE {expression}
        """,
    )


def check_freshness(
    *,
    task_id: str,
    dataset: str,
    table: str,
    timestamp_column: str,
    max_age_hours: int = 48,
) -> BigQueryCheckOperator:
    return _create(
        task_id=task_id,
        sql=f"""
            SELECT COUNT(*) > 0
            FROM `{PROJECT_ID}.{dataset}.{table}`
            WHERE {timestamp_column} >= TIMESTAMP_SUB(
                CURRENT_TIMESTAMP(),
                INTERVAL {max_age_hours} HOUR
            )
        """,
    )


def create_quality_group(
    *,
    group_id: str,
    dataset: str,
    table: str,
    not_null_columns: list[str] | None = None,
    invalid_rules: dict[str, str] | None = None,
    freshness_column: str | None = None,
    max_age_hours: int = 48,
) -> TaskGroup:
    with TaskGroup(group_id=group_id) as tg:
        table_exists = check_table_exists(
            task_id="table_exists",
            dataset=dataset,
            table=table,
        )

        row_count = check_row_count(
            task_id="row_count",
            dataset=dataset,
            table=table,
        )

        # Only check table contents after confirming the table exists.
        table_exists >> row_count

        if not_null_columns:
            for column in not_null_columns:
                check = check_not_null(
                    task_id=f"{column}_not_null",
                    dataset=dataset,
                    table=table,
                    column=column,
                )
                row_count >> check

        if invalid_rules:
            for task_name, expression in invalid_rules.items():
                check = check_invalid_value(
                    task_id=task_name,
                    dataset=dataset,
                    table=table,
                    expression=expression,
                )
                row_count >> check

        if freshness_column:
            freshness = check_freshness(
                task_id="freshness",
                dataset=dataset,
                table=table,
                timestamp_column=freshness_column,
                max_age_hours=max_age_hours,
            )
            row_count >> freshness

    return tg
