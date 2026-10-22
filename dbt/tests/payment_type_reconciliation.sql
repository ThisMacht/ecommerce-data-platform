WITH expected AS (
    SELECT o.order_purchase_date, p.payment_type,
        COUNT(*) AS total_payment_count,
        COUNT(*) FILTER (WHERE o.order_status IN ('delivered', 'shipped', 'invoiced', 'processing', 'approved', 'created')) AS eligible_payment_count,
        COUNT(DISTINCT o.order_id) FILTER (WHERE o.order_status IN ('delivered', 'shipped', 'invoiced', 'processing', 'approved', 'created')) AS eligible_order_count,
        COALESCE(SUM(p.payment_value) FILTER (WHERE o.order_status IN ('delivered', 'shipped', 'invoiced', 'processing', 'approved', 'created')), 0::numeric) AS gross_paid_amount
    FROM {{ source('raw_staging', 'order_payments') }} p
    JOIN {{ source('raw_staging', 'orders') }} o USING (order_id)
    GROUP BY o.order_purchase_date, p.payment_type
)
SELECT COALESCE(e.payment_type, m.payment_type) AS payment_type
FROM expected e
FULL JOIN {{ ref('mart_payment_type_daily') }} m USING (order_purchase_date, payment_type)
WHERE e.payment_type IS NULL OR m.payment_type IS NULL
   OR e.total_payment_count IS DISTINCT FROM m.total_payment_count
   OR e.eligible_payment_count IS DISTINCT FROM m.eligible_payment_count
   OR e.eligible_order_count IS DISTINCT FROM m.eligible_order_count
   OR e.gross_paid_amount IS DISTINCT FROM m.gross_paid_amount
