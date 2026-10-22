SELECT
    MD5(seller_id) AS seller_key,
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    first_seen_at AS created_at,
    last_seen_at AS updated_at,
    s.record_hash,
    l.location_key
FROM {{ ref('stg_sellers') }} s
LEFT JOIN {{ ref('dim_location') }} l ON s.seller_zip_code_prefix = l.zip_code_prefix
