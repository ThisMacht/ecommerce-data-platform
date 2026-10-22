{% for entity in ['product', 'seller'] %}
{% if not loop.first %}UNION ALL{% endif %}
SELECT '{{ entity }}' AS model, COALESCE(e.{{ entity }}_id, m.{{ entity }}_id) AS business_id
FROM (
    SELECT o.order_purchase_date, i.{{ entity }}_id,
        COUNT(*) AS total_item_count,
        COUNT(*) FILTER (WHERE o.order_status IN ('delivered', 'shipped', 'invoiced', 'processing', 'approved', 'created')) AS eligible_item_count,
        COUNT(DISTINCT o.order_id) FILTER (WHERE o.order_status IN ('delivered', 'shipped', 'invoiced', 'processing', 'approved', 'created')) AS eligible_order_count,
        COALESCE(SUM(i.price) FILTER (WHERE o.order_status IN ('delivered', 'shipped', 'invoiced', 'processing', 'approved', 'created')), 0::numeric) AS gmv,
        COALESCE(SUM(i.freight_value) FILTER (WHERE o.order_status IN ('delivered', 'shipped', 'invoiced', 'processing', 'approved', 'created')), 0::numeric) AS freight_amount
    FROM {{ source('raw_staging', 'order_items') }} i
    JOIN {{ source('raw_staging', 'orders') }} o USING (order_id)
    GROUP BY o.order_purchase_date, i.{{ entity }}_id
) e
FULL JOIN {{ ref('mart_' ~ entity ~ '_daily') }} m USING (order_purchase_date, {{ entity }}_id)
WHERE e.{{ entity }}_id IS NULL OR m.{{ entity }}_id IS NULL
   OR m.{{ entity }}_key IS DISTINCT FROM MD5(e.{{ entity }}_id)
   OR e.total_item_count IS DISTINCT FROM m.total_item_count
   OR e.eligible_item_count IS DISTINCT FROM m.eligible_item_count
   OR e.eligible_order_count IS DISTINCT FROM m.eligible_order_count
   OR e.gmv IS DISTINCT FROM m.gmv
   OR e.freight_amount IS DISTINCT FROM m.freight_amount
{% endfor %}
