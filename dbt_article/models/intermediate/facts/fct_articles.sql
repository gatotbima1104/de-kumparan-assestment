{{
    config(
        materialized = 'incremental',
        unique_key = 'article_id',
        incremental_strategy = 'merge',
        partition_by = {
            'field': 'created_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by = ['author_id', 'category_id']
    )
}}

SELECT
    
    -- shouldn't be here but fine for now remember 
    -- fct table contains measurable metrics and id
    article_id
    , title
    , content

    -- author
    , author_id
    , category_id

    -- dates
    , published_date
    , created_date
    , updated_date
    , deleted_date

    -- business
    , status
    , is_published
    , is_deleted
    , hours_to_publish
    , __ingestion_source

FROM {{ ref('int_article_enriched') }}
{% if is_incremental() %}

    {% if var('article_date', none) %}
        WHERE DATE(updated_at) = CAST('{{ var("article_date") }}' AS DATE) -- Manual backfill
    {% else %}

        -- Normal incremental processing
        WHERE updated_at >= (
            SELECT DATE_SUB(
                COALESCE(MAX(updated_at), DATE('2000-01-01')), 
                INTERVAL 1 DAY
            )
            FROM {{ this }}
        )

    {% endif %}
{% endif %}