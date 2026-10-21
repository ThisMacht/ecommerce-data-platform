WITH ranked AS (
    SELECT
        *,
        MIN(first_seen_at) OVER (PARTITION BY customer_unique_id) AS created_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id ORDER BY source_sequence DESC, customer_id DESC
        ) AS position
    FROM {{ ref('stg_customers') }}
)
SELECT
    MD5(customer_unique_id) AS customer_key,
    customer_unique_id,
    customer_id AS latest_customer_id,
    customer_city,
    customer_state,
    customer_zip_code_prefix,
    created_at,
    last_seen_at AS updated_at,
    MD5(JSONB_BUILD_ARRAY(customer_city, customer_state, customer_zip_code_prefix)::text) AS record_hash
FROM ranked
WHERE position = 1
