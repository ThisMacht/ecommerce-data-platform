WITH customers AS (
    SELECT DISTINCT ON (customer_unique_id) customer_unique_id, customer_id
    FROM {{ source('raw_staging', 'customers') }}
    ORDER BY customer_unique_id, source_sequence DESC, customer_id DESC
)
SELECT 'customer' AS entity, COALESCE(c.customer_unique_id, d.customer_unique_id) AS business_key
FROM customers c
FULL JOIN {{ ref('dim_customer') }} d USING (customer_unique_id)
WHERE c.customer_unique_id IS NULL OR d.customer_unique_id IS NULL
    OR d.customer_key IS DISTINCT FROM MD5(c.customer_unique_id)
    OR d.latest_customer_id IS DISTINCT FROM c.customer_id
{% for entity in ['product', 'seller'] %}
UNION ALL
SELECT '{{ entity }}', COALESCE(s.{{ entity }}_id, d.{{ entity }}_id)
FROM {{ source('raw_staging', entity ~ 's') }} s
FULL JOIN {{ ref('dim_' ~ entity) }} d USING ({{ entity }}_id)
WHERE s.{{ entity }}_id IS NULL OR d.{{ entity }}_id IS NULL
    OR d.{{ entity }}_key IS DISTINCT FROM MD5(s.{{ entity }}_id)
{% endfor %}
