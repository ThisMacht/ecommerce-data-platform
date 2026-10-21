SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    source_sequence,
    source_updated_at,
    first_seen_at,
    last_seen_at
FROM {{ source('raw_staging', 'customers') }}
