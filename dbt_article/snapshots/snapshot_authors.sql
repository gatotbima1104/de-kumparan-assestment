{% snapshot snapshot_authors %}

{{
    config(
        target_schema='snapshots_articles',
        unique_key='id',
        strategy='check',
        check_cols=['email']
    )
}}

SELECT
    id
    , name
    , email
    , created_at
FROM {{ source('article', 'authors') }}

{% endsnapshot %}
