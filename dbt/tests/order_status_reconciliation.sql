WITH expected AS (
    SELECT o.order_purchase_date, o.order_status, COUNT(*) AS order_count,
        COUNT(DISTINCT c.customer_unique_id) AS customer_count
    FROM {{ source('raw_staging', 'orders') }} o
    LEFT JOIN {{ source('raw_staging', 'customers') }} c USING (customer_id)
    GROUP BY o.order_purchase_date, o.order_status
)
SELECT COALESCE(e.order_purchase_date, m.order_purchase_date) AS order_purchase_date,
    COALESCE(e.order_status, m.order_status) AS order_status
FROM expected e FULL JOIN {{ ref('mart_order_status_daily') }} m USING (order_purchase_date, order_status)
WHERE e.order_status IS NULL OR m.order_status IS NULL
    OR e.order_count IS DISTINCT FROM m.order_count
    OR e.customer_count IS DISTINCT FROM m.customer_count
