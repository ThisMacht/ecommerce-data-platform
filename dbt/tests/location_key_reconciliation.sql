WITH zips AS (
    SELECT DISTINCT geolocation_zip_code_prefix AS zip_code_prefix
    FROM {{ source('raw_staging', 'geolocation') }}
), customers AS (
    SELECT DISTINCT ON (customer_unique_id) customer_unique_id, customer_zip_code_prefix
    FROM {{ source('raw_staging', 'customers') }}
    ORDER BY customer_unique_id, source_sequence DESC, customer_id DESC
)
SELECT 'customer' AS entity, c.customer_unique_id AS business_key
FROM customers c
LEFT JOIN zips z ON c.customer_zip_code_prefix = z.zip_code_prefix
LEFT JOIN {{ ref('dim_customer') }} d ON c.customer_unique_id = d.customer_unique_id
WHERE d.customer_key IS NULL OR d.location_key IS DISTINCT FROM MD5(z.zip_code_prefix)
UNION ALL
SELECT 'seller', s.seller_id
FROM {{ source('raw_staging', 'sellers') }} s
LEFT JOIN zips z ON s.seller_zip_code_prefix = z.zip_code_prefix
LEFT JOIN {{ ref('dim_seller') }} d ON s.seller_id = d.seller_id
WHERE d.seller_key IS NULL OR d.location_key IS DISTINCT FROM MD5(z.zip_code_prefix)
