WITH shoppers AS (
    SELECT DISTINCT customer_unique_id FROM {{ source('raw_staging', 'customers') }}
), orders AS (
    SELECT o.order_id, c.customer_unique_id, o.order_purchase_timestamp,
        o.order_status IN ('delivered', 'shipped', 'invoiced', 'processing', 'approved', 'created') AS eligible,
        (SELECT COUNT(*) FROM {{ source('raw_staging', 'order_items') }} i WHERE i.order_id = o.order_id) AS item_count,
        (SELECT SUM(price) FROM {{ source('raw_staging', 'order_items') }} i WHERE i.order_id = o.order_id) AS item_amount,
        (SELECT SUM(freight_value) FROM {{ source('raw_staging', 'order_items') }} i WHERE i.order_id = o.order_id) AS freight_amount,
        (SELECT COUNT(*) FROM {{ source('raw_staging', 'order_payments') }} p WHERE p.order_id = o.order_id) AS payment_count,
        (SELECT SUM(payment_value) FROM {{ source('raw_staging', 'order_payments') }} p WHERE p.order_id = o.order_id) AS paid
    FROM {{ source('raw_staging', 'orders') }} o
    LEFT JOIN {{ source('raw_staging', 'customers') }} c USING (customer_id)
), expected AS (
    SELECT s.customer_unique_id,
        COUNT(o.order_id) AS total_order_count,
        COUNT(o.order_id) FILTER (WHERE eligible) AS eligible_order_count,
        COUNT(o.order_id) FILTER (WHERE eligible AND paid > 0) AS paid_order_count,
        COUNT(o.order_id) FILTER (WHERE eligible AND item_count = 0) AS orders_without_items,
        COUNT(o.order_id) FILTER (WHERE eligible AND payment_count = 0) AS orders_without_payments,
        COALESCE(SUM(item_amount) FILTER (WHERE eligible), 0::numeric) AS gmv,
        COALESCE(SUM(freight_amount) FILTER (WHERE eligible), 0::numeric) AS freight_amount,
        COALESCE(SUM(paid) FILTER (WHERE eligible), 0::numeric) AS gross_paid_amount,
        ROUND(SUM(paid) FILTER (WHERE eligible AND paid > 0)
            / NULLIF(COUNT(o.order_id) FILTER (WHERE eligible AND paid > 0), 0), 2) AS average_order_value,
        MIN(order_purchase_timestamp) FILTER (WHERE eligible) AS first_eligible_purchase_timestamp,
        MAX(order_purchase_timestamp) FILTER (WHERE eligible) AS last_eligible_purchase_timestamp,
        COUNT(o.order_id) FILTER (WHERE eligible) >= 2 AS is_repeat_customer
    FROM shoppers s LEFT JOIN orders o USING (customer_unique_id)
    GROUP BY s.customer_unique_id
)
SELECT COALESCE(e.customer_unique_id, m.customer_unique_id) AS customer_unique_id
FROM expected e FULL JOIN {{ ref('mart_customer_value') }} m USING (customer_unique_id)
WHERE e.customer_unique_id IS NULL OR m.customer_unique_id IS NULL
    OR m.customer_key IS DISTINCT FROM MD5(e.customer_unique_id)
{% for field in ['total_order_count', 'eligible_order_count', 'paid_order_count', 'orders_without_items', 'orders_without_payments', 'gmv', 'freight_amount', 'gross_paid_amount', 'average_order_value', 'first_eligible_purchase_timestamp', 'last_eligible_purchase_timestamp', 'is_repeat_customer'] %}
    OR e.{{ field }} IS DISTINCT FROM m.{{ field }}
{% endfor %}
