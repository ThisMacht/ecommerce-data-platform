WITH timeline AS (
    SELECT o.order_id, f.customer_key, o.order_status, o.order_purchase_date,
        o.order_purchase_timestamp, o.order_approved_at, o.order_delivered_carrier_date,
        o.order_delivered_customer_date, o.order_estimated_delivery_date,
        (o.order_estimated_delivery_date AT TIME ZONE 'America/Sao_Paulo')::date AS estimated_delivery_date,
        (o.order_delivered_customer_date AT TIME ZONE 'America/Sao_Paulo')::date AS delivered_date,
        o.order_delivered_customer_date IS NOT NULL AS has_delivery_timestamp,
        COALESCE(o.order_delivered_carrier_date < o.order_purchase_timestamp, FALSE)
            OR COALESCE(o.order_delivered_customer_date < o.order_purchase_timestamp, FALSE)
            OR COALESCE(o.order_delivered_customer_date < o.order_delivered_carrier_date, FALSE)
            AS has_invalid_delivery_chronology
    FROM {{ ref('stg_orders') }} o
    LEFT JOIN {{ ref('fct_orders') }} f ON o.order_id = f.order_id
), observed AS (
    SELECT *, order_status = 'delivered' AND has_delivery_timestamp
        AND NOT has_invalid_delivery_chronology AS is_delivery_observed
    FROM timeline
)
SELECT o.*,
    CASE WHEN is_delivery_observed THEN delivered_date - estimated_delivery_date END AS delivery_delay_days,
    CASE WHEN is_delivery_observed THEN delivered_date > estimated_delivery_date END AS is_late_delivery,
    CASE WHEN is_delivery_observed
        THEN EXTRACT(EPOCH FROM (order_delivered_customer_date - order_purchase_timestamp)) / 3600 END AS delivery_elapsed_hours,
    CASE WHEN is_delivery_observed AND order_delivered_carrier_date IS NOT NULL
        THEN EXTRACT(EPOCH FROM (order_delivered_customer_date - order_delivered_carrier_date)) / 3600 END AS carrier_to_delivery_hours,
    pd.date_key AS order_purchase_date_key,
    ed.date_key AS estimated_delivery_date_key,
    dd.date_key AS delivered_date_key
FROM observed o
LEFT JOIN {{ ref('dim_date') }} pd ON o.order_purchase_date = pd.calendar_date
LEFT JOIN {{ ref('dim_date') }} ed ON o.estimated_delivery_date = ed.calendar_date
LEFT JOIN {{ ref('dim_date') }} dd ON o.delivered_date = dd.calendar_date
