WITH purchases AS (
    SELECT order_id, customer_key, order_purchase_date,
        ROW_NUMBER() OVER (
            PARTITION BY customer_key ORDER BY order_purchase_timestamp, order_id
        ) AS purchase_number
    FROM {{ ref('fct_orders') }}
    WHERE is_metric_eligible
)
SELECT
    order_purchase_date,
    COUNT(*) AS eligible_order_count,
    COUNT(*) FILTER (WHERE purchase_number = 1) AS first_order_count,
    COUNT(*) FILTER (WHERE purchase_number > 1) AS repeat_order_count,
    COUNT(DISTINCT customer_key) AS purchasing_customer_count,
    COUNT(DISTINCT customer_key) FILTER (WHERE purchase_number > 1) AS repeat_customer_count,
    COUNT(*) FILTER (WHERE purchase_number > 1)::numeric / NULLIF(COUNT(*), 0) AS repeat_order_rate
FROM purchases
GROUP BY order_purchase_date
