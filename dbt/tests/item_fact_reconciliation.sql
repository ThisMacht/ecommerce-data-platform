SELECT COALESCE(i.order_id, f.order_id) AS order_id
FROM {{ source('raw_staging', 'order_items') }} i
FULL JOIN {{ ref('fct_order_items') }} f USING (order_id, order_item_id)
LEFT JOIN {{ source('raw_staging', 'orders') }} o ON i.order_id = o.order_id
LEFT JOIN {{ source('raw_staging', 'customers') }} c ON o.customer_id = c.customer_id
WHERE i.order_id IS NULL OR f.order_id IS NULL
   OR i.product_id IS DISTINCT FROM f.product_id
   OR i.seller_id IS DISTINCT FROM f.seller_id
   OR MD5(i.product_id) IS DISTINCT FROM f.product_key
   OR MD5(i.seller_id) IS DISTINCT FROM f.seller_key
   OR MD5(c.customer_unique_id) IS DISTINCT FROM f.customer_key
   OR i.price IS DISTINCT FROM f.price
   OR i.freight_value IS DISTINCT FROM f.freight_value
   OR i.shipping_limit_date IS DISTINCT FROM f.shipping_limit_date
   OR o.order_purchase_date IS DISTINCT FROM f.order_purchase_date
   OR o.order_status IS DISTINCT FROM f.order_status
   OR (o.order_status IN ('delivered', 'shipped', 'invoiced', 'processing', 'approved', 'created'))
      IS DISTINCT FROM f.is_metric_eligible
