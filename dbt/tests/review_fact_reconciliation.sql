SELECT COALESCE(s.review_id, f.review_id) AS review_id, COALESCE(s.order_id, f.order_id) AS order_id
FROM {{ source('raw_staging', 'order_reviews') }} s
FULL JOIN {{ ref('fct_reviews') }} f USING (review_id, order_id)
WHERE s.review_id IS NULL OR f.review_id IS NULL
    OR s.review_score IS DISTINCT FROM f.review_score
    OR s.review_creation_date IS DISTINCT FROM f.review_creation_timestamp
    OR s.review_answer_timestamp IS DISTINCT FROM f.review_answer_timestamp
    OR (s.review_creation_date AT TIME ZONE 'America/Sao_Paulo')::date
        IS DISTINCT FROM f.review_creation_date
    OR TO_CHAR(s.review_creation_date AT TIME ZONE 'America/Sao_Paulo', 'YYYYMMDD')::integer
        IS DISTINCT FROM f.review_creation_date_key
    OR (NULLIF(BTRIM(s.review_comment_title), '') IS NOT NULL) IS DISTINCT FROM f.has_comment_title
    OR (NULLIF(BTRIM(s.review_comment_message), '') IS NOT NULL) IS DISTINCT FROM f.has_comment_message
    OR (s.review_answer_timestamp < s.review_creation_date) IS DISTINCT FROM f.has_invalid_answer_chronology
