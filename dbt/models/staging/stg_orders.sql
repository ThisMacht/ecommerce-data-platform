SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    source_sequence,
    source_updated_at,
    first_seen_at,
    last_seen_at,
    order_purchase_date
FROM {{ source('raw_staging', 'orders') }}
