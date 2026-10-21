SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    source_sequence,
    source_updated_at,
    first_seen_at,
    last_seen_at
FROM {{ source('raw_staging', 'order_payments') }}
