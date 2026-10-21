SELECT 'items' AS dataset, order_id
FROM {{ ref('stg_order_items') }} GROUP BY order_id, order_item_id HAVING COUNT(*) > 1
UNION ALL
SELECT 'payments', order_id
FROM {{ ref('stg_payments') }} GROUP BY order_id, payment_sequential HAVING COUNT(*) > 1
