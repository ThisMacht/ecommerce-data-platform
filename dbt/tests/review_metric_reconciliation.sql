WITH expected AS (
    SELECT (review_creation_date AT TIME ZONE 'America/Sao_Paulo')::date AS review_date,
        COUNT(*) AS review_count,
        COUNT(DISTINCT order_id) AS reviewed_order_count,
        SUM(review_score) AS score_sum,
        SUM(review_score)::numeric / COUNT(*) AS average_score,
        SUM(CASE WHEN review_score IN (1, 2) THEN 1 ELSE 0 END) AS low_score_count,
        SUM(CASE WHEN review_score IN (1, 2) THEN 1 ELSE 0 END)::numeric / COUNT(*) AS low_score_rate,
        SUM(CASE WHEN NULLIF(BTRIM(review_comment_title), '') IS NOT NULL
            OR NULLIF(BTRIM(review_comment_message), '') IS NOT NULL THEN 1 ELSE 0 END) AS commented_review_count,
        SUM(CASE WHEN NULLIF(BTRIM(review_comment_title), '') IS NOT NULL
            OR NULLIF(BTRIM(review_comment_message), '') IS NOT NULL THEN 1 ELSE 0 END)::numeric / COUNT(*) AS comment_rate
    FROM {{ source('raw_staging', 'order_reviews') }}
    GROUP BY (review_creation_date AT TIME ZONE 'America/Sao_Paulo')::date
)
SELECT COALESCE(e.review_date, m.review_creation_date) AS review_date
FROM expected e
FULL JOIN {{ ref('mart_review_daily') }} m ON e.review_date = m.review_creation_date
WHERE e.review_date IS NULL OR m.review_creation_date IS NULL
    OR e.review_count IS DISTINCT FROM m.review_count
    OR e.reviewed_order_count IS DISTINCT FROM m.reviewed_order_count
    OR e.score_sum IS DISTINCT FROM m.score_sum
    OR e.average_score IS DISTINCT FROM m.average_score
    OR e.low_score_count IS DISTINCT FROM m.low_score_count
    OR e.low_score_rate IS DISTINCT FROM m.low_score_rate
    OR e.commented_review_count IS DISTINCT FROM m.commented_review_count
    OR e.comment_rate IS DISTINCT FROM m.comment_rate
