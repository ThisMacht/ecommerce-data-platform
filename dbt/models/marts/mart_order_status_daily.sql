SELECT
    order_purchase_date,
    order_status,
    COUNT(*) AS order_count,
    COUNT(DISTINCT customer_key) AS customer_count
FROM {{ ref('fct_orders') }}
GROUP BY order_purchase_date, order_status
