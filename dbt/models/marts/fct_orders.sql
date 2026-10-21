SELECT
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    d.customer_key,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_purchase_date,
    o.order_status IN ('delivered', 'shipped', 'invoiced', 'processing', 'approved', 'created') AS is_metric_eligible,
    COALESCE(i.item_count, 0) AS item_count,
    COALESCE(p.payment_count, 0) AS payment_count,
    i.item_amount,
    i.freight_amount,
    p.gross_paid_amount
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
LEFT JOIN {{ ref('dim_customer') }} d ON c.customer_unique_id = d.customer_unique_id
LEFT JOIN {{ ref('int_order_item_summary') }} i ON o.order_id = i.order_id
LEFT JOIN {{ ref('int_payment_summary') }} p ON o.order_id = p.order_id
