WITH purchase_keys AS (
    SELECT order_id, order_purchase_date_key FROM {{ ref('fct_orders') }}
    UNION ALL
    SELECT order_id, order_purchase_date_key FROM {{ ref('fct_order_items') }}
    UNION ALL
    SELECT order_id, order_purchase_date_key FROM {{ ref('fct_payments') }}
    UNION ALL
    SELECT order_id, order_purchase_date_key FROM {{ ref('fct_delivery') }}
)
SELECT f.order_id
FROM purchase_keys f
LEFT JOIN {{ source('raw_staging', 'orders') }} o ON f.order_id = o.order_id
WHERE f.order_purchase_date_key IS DISTINCT FROM TO_CHAR(o.order_purchase_date, 'YYYYMMDD')::integer
UNION ALL
SELECT f.order_id
FROM {{ ref('fct_delivery') }} f
LEFT JOIN {{ source('raw_staging', 'orders') }} o ON f.order_id = o.order_id
WHERE f.estimated_delivery_date_key IS DISTINCT FROM TO_CHAR(
        (o.order_estimated_delivery_date AT TIME ZONE 'America/Sao_Paulo')::date, 'YYYYMMDD')::integer
    OR f.delivered_date_key IS DISTINCT FROM TO_CHAR(
        (o.order_delivered_customer_date AT TIME ZONE 'America/Sao_Paulo')::date, 'YYYYMMDD')::integer
