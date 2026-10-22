SELECT MD5(zip_code_prefix) AS location_key, zip_code_prefix, latitude, longitude,
    city, state, sample_count, modal_sample_count, created_at, updated_at,
    MD5(JSONB_BUILD_ARRAY(latitude, longitude, city, state, sample_count, modal_sample_count)::text) AS record_hash
FROM {{ ref('int_geolocation_by_zip') }}
