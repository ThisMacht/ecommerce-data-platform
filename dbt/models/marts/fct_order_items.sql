SELECT
    i.order_id,
    i.order_item_id,
    i.product_id,
    p.product_key,
    i.seller_id,
    s.seller_key,
    o.customer_key,
    o.order_purchase_date,
    o.order_status,
    o.is_metric_eligible,
    i.shipping_limit_date,
    i.price,
    i.freight_value,
    o.order_purchase_date_key
FROM {{ ref('stg_order_items') }} i
LEFT JOIN {{ ref('fct_orders') }} o ON i.order_id = o.order_id
LEFT JOIN {{ ref('dim_product') }} p ON i.product_id = p.product_id
LEFT JOIN {{ ref('dim_seller') }} s ON i.seller_id = s.seller_id
