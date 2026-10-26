{{
      config(
        materialized = 'table',
        partition_by={
            'field': 'created_at',
            'data_type': 'date',
            'granularity': 'day'
        }
    )
}}

SELECT

    CAST(id AS STRING) AS id
    , CAST(title AS STRING) AS title
    , CAST(content AS STRING) AS content
    , CAST(status AS STRING) AS status
    , CAST(author_id AS STRING) AS author_id
    , CAST(category_id AS numeric) AS category_id

    , CAST(published_at AS DATE) AS published_at
    , CAST(created_at AS DATE) AS created_at
    , CAST(updated_at AS DATE) AS updated_at
    , CAST(deleted_at AS DATE) AS deleted_at,

    "BATCH" AS __ingestion_source

FROM {{ source('article', 'articles') }}