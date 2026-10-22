SELECT 'payments' AS model, order_id AS business_id
FROM {{ ref('fct_payments') }}
GROUP BY order_id, payment_sequential HAVING COUNT(*) > 1
UNION ALL
SELECT 'payment_type', payment_type
FROM {{ ref('mart_payment_type_daily') }}
GROUP BY order_purchase_date, payment_type HAVING COUNT(*) > 1
