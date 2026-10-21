WITH expected AS (
    SELECT o.order_id,
        (SELECT COUNT(*) FROM {{ source('raw_staging', 'order_items') }} i WHERE i.order_id=o.order_id) AS item_count,
        (SELECT SUM(price) FROM {{ source('raw_staging', 'order_items') }} i WHERE i.order_id=o.order_id) AS item_amount,
        (SELECT SUM(freight_value) FROM {{ source('raw_staging', 'order_items') }} i WHERE i.order_id=o.order_id) AS freight_amount,
        (SELECT COUNT(*) FROM {{ source('raw_staging', 'order_payments') }} p WHERE p.order_id=o.order_id) AS payment_count,
        (SELECT SUM(payment_value) FROM {{ source('raw_staging', 'order_payments') }} p WHERE p.order_id=o.order_id) AS paid
    FROM {{ source('raw_staging', 'orders') }} o
)
SELECT COALESCE(e.order_id, f.order_id) AS order_id
FROM expected e FULL JOIN {{ ref('fct_orders') }} f USING (order_id)
WHERE e.order_id IS NULL OR f.order_id IS NULL
   OR e.item_count IS DISTINCT FROM f.item_count
   OR e.payment_count IS DISTINCT FROM f.payment_count
   OR e.item_amount IS DISTINCT FROM f.item_amount
   OR e.freight_amount IS DISTINCT FROM f.freight_amount
   OR e.paid IS DISTINCT FROM f.gross_paid_amount
