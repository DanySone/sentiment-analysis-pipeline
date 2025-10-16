SELECT
    sa.article_id,
    sa.title,
    sa.source_name,
    sa.published_at_ts,
    sent.sentiment_score
FROM
    {{ ref('stg_articles') }} sa
LEFT JOIN
    {{ source('raw_data', 'article_sentiments') }} sent
    ON sa.article_id = sent.article_id