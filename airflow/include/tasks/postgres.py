import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

from include.constant import POSTGRES_CONN_ID


# read table and return len(df)
def read_table(
    table_name: str,
    watermark_col: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    """Read a PostgreSQL table and return its data as a DataFrame"""

    engine = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_sqlalchemy_engine()
    query = f"SELECT * FROM {table_name}"
    params = {}

    if watermark_col and end:
        # if there is a start range time
        if start:
            query += f"""
                WHERE {watermark_col} >= :start
                AND {watermark_col} < :end
            """
            params = {"start": start, "end": end}
        else:
            query += f"""
                WHERE {watermark_col} < :end
            """
            params = {"end": end}

    with engine.connect() as connection:
        return pd.read_sql(text(query), connection, params=params)
