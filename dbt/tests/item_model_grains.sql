SELECT 'items' AS model, order_id AS business_id
FROM {{ ref('fct_order_items') }}
GROUP BY order_id, order_item_id HAVING COUNT(*) > 1
{% for entity in ['product', 'seller'] %}
UNION ALL
SELECT '{{ entity }}', {{ entity }}_id
FROM {{ ref('mart_' ~ entity ~ '_daily') }}
GROUP BY order_purchase_date, {{ entity }}_id HAVING COUNT(*) > 1
{% endfor %}
