SELECT geolocation_row_id, geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
    geolocation_city, geolocation_state, warning_rule_ids, source_sequence,
    source_updated_at, first_seen_at, last_seen_at, record_hash
FROM {{ source('raw_staging', 'geolocation') }}
