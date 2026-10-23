WITH observed_dates AS (
    SELECT order_purchase_date AS calendar_date FROM {{ ref('stg_orders') }}
    UNION ALL
    SELECT (order_estimated_delivery_date AT TIME ZONE 'America/Sao_Paulo')::date
    FROM {{ ref('stg_orders') }}
    UNION ALL
    SELECT (order_delivered_customer_date AT TIME ZONE 'America/Sao_Paulo')::date
    FROM {{ ref('stg_orders') }}
    UNION ALL
    SELECT (review_creation_timestamp AT TIME ZONE 'America/Sao_Paulo')::date
    FROM {{ ref('stg_reviews') }}
), bounds AS (
    SELECT MIN(calendar_date) AS first_date, MAX(calendar_date) AS last_date
    FROM observed_dates
), calendar AS (
    SELECT first_date + day_offset AS calendar_date
    FROM bounds CROSS JOIN LATERAL generate_series(0, last_date - first_date) AS day_offset
)
SELECT
    (EXTRACT(YEAR FROM calendar_date) * 10000
        + EXTRACT(MONTH FROM calendar_date) * 100
        + EXTRACT(DAY FROM calendar_date))::integer AS date_key,
    calendar_date,
    EXTRACT(YEAR FROM calendar_date)::integer AS calendar_year,
    EXTRACT(QUARTER FROM calendar_date)::integer AS calendar_quarter,
    EXTRACT(MONTH FROM calendar_date)::integer AS calendar_month,
    EXTRACT(DAY FROM calendar_date)::integer AS day_of_month,
    EXTRACT(ISOYEAR FROM calendar_date)::integer AS iso_year,
    EXTRACT(WEEK FROM calendar_date)::integer AS iso_week,
    EXTRACT(ISODOW FROM calendar_date)::integer AS iso_day_of_week,
    EXTRACT(ISODOW FROM calendar_date) IN (6, 7) AS is_weekend
FROM calendar
