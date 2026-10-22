SELECT COALESCE(p.order_id, f.order_id) AS order_id
FROM {{ source('raw_staging', 'order_payments') }} p
FULL JOIN {{ ref('fct_payments') }} f USING (order_id, payment_sequential)
LEFT JOIN {{ source('raw_staging', 'orders') }} o ON p.order_id = o.order_id
LEFT JOIN {{ source('raw_staging', 'customers') }} c ON o.customer_id = c.customer_id
WHERE p.order_id IS NULL OR f.order_id IS NULL
   OR p.payment_type IS DISTINCT FROM f.payment_type
   OR p.payment_installments IS DISTINCT FROM f.payment_installments
   OR p.payment_value IS DISTINCT FROM f.payment_value
   OR MD5(c.customer_unique_id) IS DISTINCT FROM f.customer_key
   OR o.order_purchase_date IS DISTINCT FROM f.order_purchase_date
   OR o.order_status IS DISTINCT FROM f.order_status
   OR (o.order_status IN ('delivered', 'shipped', 'invoiced', 'processing', 'approved', 'created'))
      IS DISTINCT FROM f.is_metric_eligible
