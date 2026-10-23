SELECT
    review_creation_date,
    COUNT(*) AS review_count,
    COUNT(DISTINCT order_id) AS reviewed_order_count,
    SUM(review_score) AS score_sum,
    AVG(review_score) AS average_score,
    COUNT(*) FILTER (WHERE review_score <= 2) AS low_score_count,
    COUNT(*) FILTER (WHERE review_score <= 2)::numeric / NULLIF(COUNT(*), 0) AS low_score_rate,
    COUNT(*) FILTER (WHERE has_comment_title OR has_comment_message) AS commented_review_count,
    COUNT(*) FILTER (WHERE has_comment_title OR has_comment_message)::numeric / NULLIF(COUNT(*), 0) AS comment_rate
FROM {{ ref('fct_reviews') }}
GROUP BY review_creation_date
