WITH source AS (
    SELECT o.*,
        order_status = 'delivered' AND order_delivered_customer_date IS NOT NULL
            AND order_delivered_customer_date >= order_purchase_timestamp
            AND (order_delivered_carrier_date IS NULL OR (
                order_delivered_carrier_date >= order_purchase_timestamp
                AND order_delivered_customer_date >= order_delivered_carrier_date)) AS observed,
        CASE WHEN order_delivered_carrier_date < order_purchase_timestamp THEN TRUE
             WHEN order_delivered_customer_date < order_purchase_timestamp THEN TRUE
             WHEN order_delivered_customer_date < order_delivered_carrier_date THEN TRUE
             ELSE FALSE END AS invalid,
        (order_delivered_customer_date AT TIME ZONE 'America/Sao_Paulo')::date
            - (order_estimated_delivery_date AT TIME ZONE 'America/Sao_Paulo')::date AS delay
    FROM {{ source('raw_staging', 'orders') }} o
), expected AS (
    SELECT order_purchase_date, COUNT(*) AS total_order_count,
        COUNT(*) FILTER (WHERE order_status='delivered') AS delivered_order_count,
        COUNT(*) FILTER (WHERE order_status='delivered' AND order_delivered_customer_date IS NULL) AS delivered_missing_timestamp_count,
        COUNT(*) FILTER (WHERE invalid) AS invalid_chronology_count,
        COUNT(*) FILTER (WHERE observed) AS observed_delivery_count,
        COUNT(*) FILTER (WHERE observed AND order_delivered_carrier_date IS NOT NULL) AS transit_observation_count,
        COUNT(*) FILTER (WHERE observed AND delay>0) AS late_delivery_count,
        COUNT(*) FILTER (WHERE observed AND delay<=0) AS on_time_delivery_count,
        COUNT(*) FILTER (WHERE observed AND delay>0)::numeric
            / NULLIF(COUNT(*) FILTER (WHERE observed),0) AS late_delivery_rate,
        AVG(EXTRACT(EPOCH FROM (order_delivered_customer_date-order_purchase_timestamp))/3600)
            FILTER (WHERE observed) AS avg_delivery_hours,
        AVG(EXTRACT(EPOCH FROM (order_delivered_customer_date-order_delivered_carrier_date))/3600)
            FILTER (WHERE observed AND order_delivered_carrier_date IS NOT NULL) AS avg_carrier_to_delivery_hours
    FROM source GROUP BY order_purchase_date
)
SELECT COALESCE(e.order_purchase_date,m.order_purchase_date) AS order_purchase_date
FROM expected e FULL JOIN {{ ref('mart_delivery_daily') }} m USING (order_purchase_date)
WHERE e.order_purchase_date IS NULL OR m.order_purchase_date IS NULL
{% for column in ['total_order_count', 'delivered_order_count', 'delivered_missing_timestamp_count', 'invalid_chronology_count', 'observed_delivery_count', 'transit_observation_count', 'late_delivery_count', 'on_time_delivery_count', 'late_delivery_rate', 'avg_delivery_hours', 'avg_carrier_to_delivery_hours'] %}
   OR e.{{ column }} IS DISTINCT FROM m.{{ column }}
{% endfor %}
