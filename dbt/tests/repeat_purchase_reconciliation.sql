WITH eligible AS (
    SELECT o.order_id, o.order_purchase_timestamp, o.order_purchase_date, c.customer_unique_id
    FROM {{ source('raw_staging', 'orders') }} o
    LEFT JOIN {{ source('raw_staging', 'customers') }} c USING (customer_id)
    WHERE o.order_status IN ('delivered', 'shipped', 'invoiced', 'processing', 'approved', 'created')
), first_orders AS (
    SELECT DISTINCT ON (customer_unique_id) customer_unique_id, order_id
    FROM eligible ORDER BY customer_unique_id, order_purchase_timestamp, order_id
), expected AS (
    SELECT e.order_purchase_date,
        COUNT(*) AS eligible_order_count,
        SUM(CASE WHEN e.order_id = f.order_id THEN 1 ELSE 0 END) AS first_order_count,
        SUM(CASE WHEN e.order_id <> f.order_id THEN 1 ELSE 0 END) AS repeat_order_count,
        COUNT(DISTINCT e.customer_unique_id) AS purchasing_customer_count,
        COUNT(DISTINCT e.customer_unique_id) FILTER (WHERE e.order_id <> f.order_id) AS repeat_customer_count,
        SUM(CASE WHEN e.order_id <> f.order_id THEN 1 ELSE 0 END)::numeric / COUNT(*) AS repeat_order_rate
    FROM eligible e JOIN first_orders f USING (customer_unique_id)
    GROUP BY e.order_purchase_date
)
SELECT COALESCE(e.order_purchase_date, m.order_purchase_date) AS order_purchase_date
FROM expected e FULL JOIN {{ ref('mart_repeat_purchase_daily') }} m USING (order_purchase_date)
WHERE e.order_purchase_date IS NULL OR m.order_purchase_date IS NULL
{% for field in ['eligible_order_count', 'first_order_count', 'repeat_order_count', 'purchasing_customer_count', 'repeat_customer_count', 'repeat_order_rate'] %}
    OR e.{{ field }} IS DISTINCT FROM m.{{ field }}
{% endfor %}
