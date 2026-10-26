{{
    config(
        materialized = 'incremental',
        unique_key = 'article_id',
        incremental_strategy = 'merge',
        partition_by = {
            'field': 'created_date',
            'data_type': 'date',
            'granularity': 'day'
        }
    )
}}

WITH authors AS (
    SELECT 
        id
        , name
        , email
    FROM {{ ref('stg_authors') }}
)
, categories AS (
    SELECT 
        id
        , name
    FROM {{ ref('stg_categories') }}
)
, articles AS (
    SELECT *
    FROM {{ ref('stg_articles') }}
    {% if is_incremental() %}

        {% if var('article_date', none) %}
            -- Manual backfill: process a specific update date
            WHERE DATE(updated_at) = CAST('{{ var("article_date") }}' AS DATE)
        {% else %}
            -- Normal incremental run: process recent changes
            WHERE updated_at >= (
                SELECT DATE_SUB(
                    COALESCE(MAX(updated_at), DATE '2000-01-01'),
                    INTERVAL 1 DAY
                )
                FROM {{ this }}
            )
        {% endif %}
    {% endif %}
)

SELECT

    ar.id AS article_id
    , ar.title
    , ar.content
    , ar.status

    -- author
    , ar.author_id
    , au.name AS author_name
    , au.email AS author_email

    -- category
    , ar.category_id
    , ca.name AS category_name

    -- dates
    , ar.published_at
    , DATE(ar.published_at) AS published_date
    , ar.created_at
    , DATE(ar.created_at) AS created_date
    , ar.updated_at
    , DATE(ar.updated_at) AS updated_date
    , ar.deleted_at
    , DATE(ar.deleted_at) AS deleted_date
    
    -- source
    , ar.__ingestion_source

    -- business
    , ar.status = 'PUBLISHED' AND ar.published_at IS NOT NULL AS is_published
    , CASE
        WHEN ar.published_at IS NOT NULL AND ar.published_at >= ar.created_at
        THEN TIMESTAMP_DIFF(
            ar.published_at,
            ar.created_at,
            HOUR
        )
    END AS hours_to_publish
    , ar.deleted_at IS NOT NULL AS is_deleted

FROM articles AS ar

LEFT JOIN authors AS au
    ON ar.author_id = au.id

LEFT JOIN categories AS ca
    ON ar.category_id = ca.id
