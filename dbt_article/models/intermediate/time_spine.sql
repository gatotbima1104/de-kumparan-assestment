
{{ config(materialized='table') }}

SELECT date_day
FROM UNNEST(
    GENERATE_DATE_ARRAY(
        DATE '2016-01-01'
        , DATE_ADD(CURRENT_DATE(), INTERVAL 1 YEAR)
        , INTERVAL 1 DAY
    )
) AS date_day