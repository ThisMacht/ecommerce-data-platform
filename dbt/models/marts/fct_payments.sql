SELECT
    p.order_id,
    p.payment_sequential,
    o.customer_key,
    o.order_purchase_date,
    o.order_status,
    o.is_metric_eligible,
    p.payment_type,
    p.payment_installments,
    p.payment_value
FROM {{ ref('stg_payments') }} p
LEFT JOIN {{ ref('fct_orders') }} o ON p.order_id = o.order_id
