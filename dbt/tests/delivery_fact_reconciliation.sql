WITH source AS (
    SELECT o.*, MD5(c.customer_unique_id) AS customer_key,
        CASE WHEN o.order_delivered_carrier_date < o.order_purchase_timestamp THEN TRUE
             WHEN o.order_delivered_customer_date < o.order_purchase_timestamp THEN TRUE
             WHEN o.order_delivered_customer_date < o.order_delivered_carrier_date THEN TRUE
             ELSE FALSE END AS invalid
    FROM {{ source('raw_staging', 'orders') }} o
    LEFT JOIN {{ source('raw_staging', 'customers') }} c USING (customer_id)
), expected AS (
    SELECT *, order_status = 'delivered' AND order_delivered_customer_date IS NOT NULL
        AND NOT invalid AS observed,
        (order_estimated_delivery_date AT TIME ZONE 'America/Sao_Paulo')::date AS estimated,
        (order_delivered_customer_date AT TIME ZONE 'America/Sao_Paulo')::date AS actual
    FROM source
)
SELECT COALESCE(e.order_id, f.order_id) AS order_id
FROM expected e FULL JOIN {{ ref('fct_delivery') }} f USING (order_id)
WHERE e.order_id IS NULL OR f.order_id IS NULL
{% for column in ['customer_key', 'order_status', 'order_purchase_date', 'order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date'] %}
   OR e.{{ column }} IS DISTINCT FROM f.{{ column }}
{% endfor %}
   OR e.estimated IS DISTINCT FROM f.estimated_delivery_date
   OR e.actual IS DISTINCT FROM f.delivered_date
   OR (e.order_delivered_customer_date IS NOT NULL) IS DISTINCT FROM f.has_delivery_timestamp
   OR e.invalid IS DISTINCT FROM f.has_invalid_delivery_chronology
   OR e.observed IS DISTINCT FROM f.is_delivery_observed
   OR (CASE WHEN e.observed THEN e.actual - e.estimated END) IS DISTINCT FROM f.delivery_delay_days
   OR (CASE WHEN e.observed THEN e.actual > e.estimated END) IS DISTINCT FROM f.is_late_delivery
   OR (CASE WHEN e.observed THEN EXTRACT(EPOCH FROM (e.order_delivered_customer_date-e.order_purchase_timestamp))/3600 END)
      IS DISTINCT FROM f.delivery_elapsed_hours
   OR (CASE WHEN e.observed AND e.order_delivered_carrier_date IS NOT NULL
            THEN EXTRACT(EPOCH FROM (e.order_delivered_customer_date-e.order_delivered_carrier_date))/3600 END)
      IS DISTINCT FROM f.carrier_to_delivery_hours
