
{{
    config(
        materialized = 'table',
        partition_by = {
            'field': 'created_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by = ['author_id', 'category_id']
    )
}}

WITH articles AS (
    SELECT *
    FROM {{ ref('fct_articles') }}
)

, authors AS (
    SELECT
        id AS author_id
        , name AS author_name
    FROM {{ ref('dim_author') }}
)

, categories AS (
    SELECT
        id AS category_id
        , name AS category_name
    FROM {{ ref('dim_category') }}
)

SELECT
    ar.created_date

    -- Author dimension
    , ar.author_id
    , au.author_name

    -- Category dimension
    , ar.category_id
    , ca.category_name

    -- Article status
    , ar.status

    -- Article metrics
    , COUNT(*) AS total_articles
    , COUNTIF(ar.is_published) AS published_articles
    , COUNTIF(ar.status = 'DRAFT') AS draft_articles
    , COUNTIF(ar.status = 'ARCHIVED') AS archived_articles
    , COUNTIF(ar.is_deleted) AS deleted_articles

    -- Publishing performance
    , ROUND(AVG(ar.hours_to_publish), 2) AS avg_hours_to_publish

    -- Ingestion breakdown
    , COUNTIF(ar.__ingestion_source = 'BATCH') AS batch_articles
    , COUNTIF(ar.__ingestion_source = 'STREAM') AS stream_articles

FROM articles AS ar

LEFT JOIN authors AS au
    ON ar.author_id = au.author_id

LEFT JOIN categories AS ca
    ON ar.category_id = ca.category_id

GROUP BY
    ar.created_date,
    ar.author_id,
    au.author_name,
    ar.category_id,
    ca.category_name,
    ar.status