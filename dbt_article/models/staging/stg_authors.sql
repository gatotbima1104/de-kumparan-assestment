{{
    config(materialized = 'table')
}}

SELECT
    CAST(id AS STRING) AS id
    , CAST(name AS STRING) AS name
    , CAST(email AS STRING) AS email
    , CAST(created_at AS DATE) AS created_at

FROM {{ ref('snapshot_authors') }}
WHERE dbt_valid_to IS NULL