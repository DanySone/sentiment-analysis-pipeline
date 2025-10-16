SELECT
    DATE_TRUNC('day', published_at_ts)::date AS article_date,
    source_name,
    AVG(sentiment_score) AS average_sentiment,
    COUNT(article_id) AS number_of_articles
FROM
    {{ ref('fct_articles_with_sentiment') }}
GROUP BY
    1, 2
ORDER BY
    article_date DESC,
    source_name