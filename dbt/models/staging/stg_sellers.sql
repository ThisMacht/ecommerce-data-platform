SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    source_sequence,
    source_updated_at,
    first_seen_at,
    last_seen_at,
    record_hash
FROM {{ source('raw_staging', 'sellers') }}
