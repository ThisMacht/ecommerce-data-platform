SELECT
    order_purchase_date,
    COUNT(*) AS total_order_count,
    COUNT(*) FILTER (WHERE is_metric_eligible) AS eligible_order_count,
    COUNT(*) FILTER (WHERE is_metric_eligible AND item_count = 0) AS orders_without_items,
    COUNT(*) FILTER (WHERE is_metric_eligible AND payment_count = 0) AS orders_without_payments,
    COALESCE(SUM(item_amount) FILTER (WHERE is_metric_eligible), 0::numeric) AS gmv,
    COALESCE(SUM(freight_amount) FILTER (WHERE is_metric_eligible), 0::numeric) AS freight_amount,
    COALESCE(SUM(gross_paid_amount) FILTER (WHERE is_metric_eligible), 0::numeric) AS gross_paid_amount
FROM {{ ref('fct_orders') }}
GROUP BY order_purchase_date
