WITH reviews AS (
    SELECT review_id, order_id, review_score, review_creation_timestamp,
        review_answer_timestamp,
        (review_creation_timestamp AT TIME ZONE 'America/Sao_Paulo')::date AS review_creation_date,
        NULLIF(BTRIM(review_comment_title), '') IS NOT NULL AS has_comment_title,
        NULLIF(BTRIM(review_comment_message), '') IS NOT NULL AS has_comment_message,
        review_answer_timestamp < review_creation_timestamp AS has_invalid_answer_chronology
    FROM {{ ref('stg_reviews') }}
)
SELECT r.*, d.date_key AS review_creation_date_key
FROM reviews r
LEFT JOIN {{ ref('dim_date') }} d ON r.review_creation_date = d.calendar_date
