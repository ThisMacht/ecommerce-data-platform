SELECT
    d.customer_key,
    d.customer_unique_id,
    COUNT(o.order_id) AS total_order_count,
    COUNT(*) FILTER (WHERE o.is_metric_eligible) AS eligible_order_count,
    COUNT(*) FILTER (WHERE o.is_metric_eligible AND o.gross_paid_amount > 0) AS paid_order_count,
    COUNT(*) FILTER (WHERE o.is_metric_eligible AND o.item_count = 0) AS orders_without_items,
    COUNT(*) FILTER (WHERE o.is_metric_eligible AND o.payment_count = 0) AS orders_without_payments,
    COALESCE(SUM(o.item_amount) FILTER (WHERE o.is_metric_eligible), 0::numeric) AS gmv,
    COALESCE(SUM(o.freight_amount) FILTER (WHERE o.is_metric_eligible), 0::numeric) AS freight_amount,
    COALESCE(SUM(o.gross_paid_amount) FILTER (WHERE o.is_metric_eligible), 0::numeric) AS gross_paid_amount,
    ROUND(SUM(o.gross_paid_amount) FILTER (WHERE o.is_metric_eligible AND o.gross_paid_amount > 0)
        / NULLIF(COUNT(*) FILTER (WHERE o.is_metric_eligible AND o.gross_paid_amount > 0), 0), 2) AS average_order_value,
    MIN(o.order_purchase_timestamp) FILTER (WHERE o.is_metric_eligible) AS first_eligible_purchase_timestamp,
    MAX(o.order_purchase_timestamp) FILTER (WHERE o.is_metric_eligible) AS last_eligible_purchase_timestamp,
    COUNT(*) FILTER (WHERE o.is_metric_eligible) >= 2 AS is_repeat_customer
FROM {{ ref('dim_customer') }} d
LEFT JOIN {{ ref('fct_orders') }} o ON d.customer_key = o.customer_key
GROUP BY d.customer_key, d.customer_unique_id
