SELECT
    order_purchase_date,
    seller_id,
    seller_key,
    COUNT(*) AS total_item_count,
    COUNT(*) FILTER (WHERE is_metric_eligible) AS eligible_item_count,
    COUNT(DISTINCT order_id) FILTER (WHERE is_metric_eligible) AS eligible_order_count,
    COALESCE(SUM(price) FILTER (WHERE is_metric_eligible), 0::numeric) AS gmv,
    COALESCE(SUM(freight_value) FILTER (WHERE is_metric_eligible), 0::numeric) AS freight_amount
FROM {{ ref('fct_order_items') }}
GROUP BY order_purchase_date, seller_id, seller_key
