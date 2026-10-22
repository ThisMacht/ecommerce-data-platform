WITH source_dates AS (
    SELECT order_purchase_date AS calendar_date FROM {{ source('raw_staging', 'orders') }}
    UNION ALL
    SELECT (order_estimated_delivery_date AT TIME ZONE 'America/Sao_Paulo')::date
    FROM {{ source('raw_staging', 'orders') }}
    UNION ALL
    SELECT (order_delivered_customer_date AT TIME ZONE 'America/Sao_Paulo')::date
    FROM {{ source('raw_staging', 'orders') }}
), bounds AS (
    SELECT MIN(calendar_date) AS first_date, MAX(calendar_date) AS last_date FROM source_dates
), expected AS (
    SELECT first_date + n AS calendar_date
    FROM bounds CROSS JOIN LATERAL generate_series(0, last_date - first_date) AS n
)
SELECT COALESCE(e.calendar_date, d.calendar_date) AS calendar_date
FROM expected e
FULL JOIN {{ ref('dim_date') }} d ON e.calendar_date = d.calendar_date
WHERE e.calendar_date IS NULL OR d.calendar_date IS NULL
    OR d.date_key IS DISTINCT FROM TO_CHAR(e.calendar_date, 'YYYYMMDD')::integer
    OR d.calendar_year IS DISTINCT FROM TO_CHAR(e.calendar_date, 'YYYY')::integer
    OR d.calendar_quarter IS DISTINCT FROM TO_CHAR(e.calendar_date, 'Q')::integer
    OR d.calendar_month IS DISTINCT FROM TO_CHAR(e.calendar_date, 'MM')::integer
    OR d.day_of_month IS DISTINCT FROM TO_CHAR(e.calendar_date, 'DD')::integer
    OR d.iso_year IS DISTINCT FROM TO_CHAR(e.calendar_date, 'IYYY')::integer
    OR d.iso_week IS DISTINCT FROM TO_CHAR(e.calendar_date, 'IW')::integer
    OR d.iso_day_of_week IS DISTINCT FROM TO_CHAR(e.calendar_date, 'ID')::integer
    OR d.is_weekend IS DISTINCT FROM (TO_CHAR(e.calendar_date, 'ID') IN ('6', '7'))
