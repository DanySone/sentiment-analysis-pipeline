-- Fichier : dbt_project/models/staging/stg_articles.sql

SELECT
    id AS article_id,
    source_name,
    author,
    title,
    description,
    url,
    published_at::timestamp AS published_at_ts,
    inserted_at::timestamp AS inserted_at_ts
FROM
    {{ source('raw_data', 'raw_articles') }}
WHERE
    title IS NOT NULL