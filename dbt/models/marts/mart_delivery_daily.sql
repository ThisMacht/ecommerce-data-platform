SELECT order_purchase_date,
    COUNT(*) AS total_order_count,
    COUNT(*) FILTER (WHERE order_status = 'delivered') AS delivered_order_count,
    COUNT(*) FILTER (WHERE order_status = 'delivered' AND NOT has_delivery_timestamp) AS delivered_missing_timestamp_count,
    COUNT(*) FILTER (WHERE has_invalid_delivery_chronology) AS invalid_chronology_count,
    COUNT(*) FILTER (WHERE is_delivery_observed) AS observed_delivery_count,
    COUNT(carrier_to_delivery_hours) AS transit_observation_count,
    COUNT(*) FILTER (WHERE is_late_delivery) AS late_delivery_count,
    COUNT(*) FILTER (WHERE is_late_delivery = FALSE) AS on_time_delivery_count,
    COUNT(*) FILTER (WHERE is_late_delivery)::numeric
        / NULLIF(COUNT(*) FILTER (WHERE is_delivery_observed), 0) AS late_delivery_rate,
    AVG(delivery_elapsed_hours) AS avg_delivery_hours,
    AVG(carrier_to_delivery_hours) AS avg_carrier_to_delivery_hours
FROM {{ ref('fct_delivery') }}
GROUP BY order_purchase_date
