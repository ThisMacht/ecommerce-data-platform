SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    source_sequence,
    source_updated_at,
    first_seen_at,
    last_seen_at
FROM {{ source('raw_staging', 'order_items') }}
