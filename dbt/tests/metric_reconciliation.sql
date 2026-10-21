WITH orders AS (
    SELECT * FROM {{ source('raw_staging', 'orders') }}
    WHERE order_status IN ('delivered', 'shipped', 'invoiced', 'processing', 'approved', 'created')
), dates AS (
    SELECT DISTINCT order_purchase_date FROM {{ source('raw_staging', 'orders') }}
), items AS (
    SELECT o.order_purchase_date, SUM(i.price) AS gmv, SUM(i.freight_value) AS freight
    FROM orders o JOIN {{ source('raw_staging', 'order_items') }} i USING (order_id)
    GROUP BY o.order_purchase_date
), payments AS (
    SELECT o.order_purchase_date, p.order_id, SUM(p.payment_value) AS amount
    FROM orders o JOIN {{ source('raw_staging', 'order_payments') }} p USING (order_id)
    GROUP BY o.order_purchase_date, p.order_id
), paid AS (
    SELECT order_purchase_date, SUM(amount) AS gross_paid,
        COUNT(*) FILTER (WHERE amount > 0) AS paid_count,
        SUM(amount) FILTER (WHERE amount > 0) AS paid_amount,
        ROUND(SUM(amount) FILTER (WHERE amount > 0) / NULLIF(COUNT(*) FILTER (WHERE amount > 0), 0), 2) AS aov
    FROM payments GROUP BY order_purchase_date
)
SELECT d.order_purchase_date
FROM dates d
LEFT JOIN items i USING (order_purchase_date)
LEFT JOIN paid p USING (order_purchase_date)
LEFT JOIN {{ ref('mart_revenue_daily') }} r USING (order_purchase_date)
LEFT JOIN {{ ref('mart_average_order_value') }} a USING (order_purchase_date)
WHERE r.order_purchase_date IS NULL OR a.order_purchase_date IS NULL
   OR r.gmv IS DISTINCT FROM COALESCE(i.gmv, 0::numeric)
   OR r.freight_amount IS DISTINCT FROM COALESCE(i.freight, 0::numeric)
   OR r.gross_paid_amount IS DISTINCT FROM COALESCE(p.gross_paid, 0::numeric)
   OR a.paid_order_count IS DISTINCT FROM COALESCE(p.paid_count, 0::bigint)
   OR a.paid_amount IS DISTINCT FROM COALESCE(p.paid_amount, 0::numeric)
   OR a.average_order_value IS DISTINCT FROM p.aov
