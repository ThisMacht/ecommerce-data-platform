WITH observations AS (
    SELECT geolocation_zip_code_prefix AS zip_code_prefix, COUNT(*)::integer AS n,
        ARRAY_AGG(geolocation_lat ORDER BY geolocation_lat) AS latitudes,
        ARRAY_AGG(geolocation_lng ORDER BY geolocation_lng) AS longitudes,
        MIN(first_seen_at) AS created_at, MAX(last_seen_at) AS updated_at
    FROM {{ source('raw_staging', 'geolocation') }} GROUP BY geolocation_zip_code_prefix
), modes AS (
    SELECT DISTINCT ON (geolocation_zip_code_prefix)
        geolocation_zip_code_prefix AS zip_code_prefix,
        geolocation_city AS city, geolocation_state AS state, COUNT(*) AS modal_sample_count
    FROM {{ source('raw_staging', 'geolocation') }}
    WHERE geolocation_city IS NOT NULL AND geolocation_state IN (
        'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA',
        'PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO')
    GROUP BY geolocation_zip_code_prefix, geolocation_city, geolocation_state
    ORDER BY geolocation_zip_code_prefix, COUNT(*) DESC,
        geolocation_state COLLATE "C", geolocation_city COLLATE "C"
), expected AS (
    SELECT o.zip_code_prefix, o.n AS sample_count,
        TRIM_SCALE((latitudes[(n+1)/2] + latitudes[(n+2)/2]) * 0.5) AS latitude,
        TRIM_SCALE((longitudes[(n+1)/2] + longitudes[(n+2)/2]) * 0.5) AS longitude,
        m.city, m.state, COALESCE(m.modal_sample_count, 0) AS modal_sample_count,
        o.created_at, o.updated_at
    FROM observations o LEFT JOIN modes m USING (zip_code_prefix)
)
SELECT COALESCE(e.zip_code_prefix, d.zip_code_prefix) AS zip_code_prefix
FROM expected e FULL JOIN {{ ref('dim_location') }} d USING (zip_code_prefix)
WHERE e.zip_code_prefix IS NULL OR d.zip_code_prefix IS NULL
    OR d.location_key IS DISTINCT FROM MD5(e.zip_code_prefix)
    OR ROW(d.latitude,d.longitude,d.city,d.state,d.sample_count,d.modal_sample_count,d.created_at,d.updated_at)
        IS DISTINCT FROM ROW(e.latitude,e.longitude,e.city,e.state,e.sample_count,e.modal_sample_count,e.created_at,e.updated_at)
    OR d.record_hash IS DISTINCT FROM MD5(JSONB_BUILD_ARRAY(e.latitude,e.longitude,e.city,e.state,e.sample_count,e.modal_sample_count)::text)
