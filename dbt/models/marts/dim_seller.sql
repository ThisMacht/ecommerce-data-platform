SELECT
    MD5(seller_id) AS seller_key,
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    first_seen_at AS created_at,
    last_seen_at AS updated_at,
    record_hash
FROM {{ ref('stg_sellers') }}
