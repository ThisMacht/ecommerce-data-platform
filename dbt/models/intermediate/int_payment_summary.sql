SELECT
    order_id,
    COUNT(*) AS payment_count,
    SUM(payment_value) AS gross_paid_amount
FROM {{ ref('stg_payments') }}
GROUP BY order_id
