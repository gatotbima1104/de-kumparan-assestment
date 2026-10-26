{{
    config(materialized = 'table')
}}

SELECT
    CAST(id AS INT) AS id
    , CAST(name AS STRING) AS name
FROM {{ source('article', 'categories') }}