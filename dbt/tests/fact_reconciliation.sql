WITH expected AS (
    SELECT o.order_id, o.customer_id, c.customer_unique_id,
        MD5(c.customer_unique_id) AS customer_key,
        o.order_status, o.order_purchase_timestamp, o.order_purchase_date,
        o.order_status IN ('delivered', 'shipped', 'invoiced', 'processing', 'approved', 'created') AS is_metric_eligible,
        (SELECT COUNT(*) FROM {{ source('raw_staging', 'order_items') }} i WHERE i.order_id=o.order_id) AS item_count,
        (SELECT SUM(price) FROM {{ source('raw_staging', 'order_items') }} i WHERE i.order_id=o.order_id) AS item_amount,
        (SELECT SUM(freight_value) FROM {{ source('raw_staging', 'order_items') }} i WHERE i.order_id=o.order_id) AS freight_amount,
        (SELECT COUNT(*) FROM {{ source('raw_staging', 'order_payments') }} p WHERE p.order_id=o.order_id) AS payment_count,
        (SELECT SUM(payment_value) FROM {{ source('raw_staging', 'order_payments') }} p WHERE p.order_id=o.order_id) AS paid
    FROM {{ source('raw_staging', 'orders') }} o
    LEFT JOIN {{ source('raw_staging', 'customers') }} c USING (customer_id)
)
SELECT COALESCE(e.order_id, f.order_id) AS order_id
FROM expected e FULL JOIN {{ ref('fct_orders') }} f USING (order_id)
WHERE e.order_id IS NULL OR f.order_id IS NULL
{% for column in ['customer_id', 'customer_unique_id', 'customer_key', 'order_status', 'order_purchase_timestamp', 'order_purchase_date', 'is_metric_eligible'] %}
   OR e.{{ column }} IS DISTINCT FROM f.{{ column }}
{% endfor %}
   OR e.item_count IS DISTINCT FROM f.item_count
   OR e.payment_count IS DISTINCT FROM f.payment_count
   OR e.item_amount IS DISTINCT FROM f.item_amount
   OR e.freight_amount IS DISTINCT FROM f.freight_amount
   OR e.paid IS DISTINCT FROM f.gross_paid_amount
