SELECT order_purchase_date, order_status
FROM {{ ref('mart_order_status_daily') }}
GROUP BY order_purchase_date, order_status HAVING COUNT(*) > 1
