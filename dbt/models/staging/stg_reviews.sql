SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date AS review_creation_timestamp,
    review_answer_timestamp,
    source_sequence,
    source_updated_at
FROM {{ source('raw_staging', 'order_reviews') }}
