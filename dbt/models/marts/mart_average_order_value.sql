SELECT
    order_purchase_date,
    COUNT(*) FILTER (WHERE is_metric_eligible AND gross_paid_amount > 0) AS paid_order_count,
    COALESCE(SUM(gross_paid_amount) FILTER (WHERE is_metric_eligible AND gross_paid_amount > 0), 0::numeric) AS paid_amount,
    ROUND(
        SUM(gross_paid_amount) FILTER (WHERE is_metric_eligible AND gross_paid_amount > 0)
        / NULLIF(COUNT(*) FILTER (WHERE is_metric_eligible AND gross_paid_amount > 0), 0), 2
    ) AS average_order_value
FROM {{ ref('fct_orders') }}
GROUP BY order_purchase_date
