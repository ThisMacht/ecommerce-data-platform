SELECT
    order_purchase_date,
    payment_type,
    COUNT(*) AS total_payment_count,
    COUNT(*) FILTER (WHERE is_metric_eligible) AS eligible_payment_count,
    COUNT(DISTINCT order_id) FILTER (WHERE is_metric_eligible) AS eligible_order_count,
    COALESCE(SUM(payment_value) FILTER (WHERE is_metric_eligible), 0::numeric) AS gross_paid_amount
FROM {{ ref('fct_payments') }}
GROUP BY order_purchase_date, payment_type
