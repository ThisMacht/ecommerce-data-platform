SELECT
    order_id,
    COUNT(*) AS item_count,
    SUM(price) AS item_amount,
    SUM(freight_value) AS freight_amount
FROM {{ ref('stg_order_items') }}
GROUP BY order_id
