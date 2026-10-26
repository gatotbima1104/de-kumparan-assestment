SELECT
    id
    , name
    , email
    , created_at

FROM {{ ref('stg_authors') }}