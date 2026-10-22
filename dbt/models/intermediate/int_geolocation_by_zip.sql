WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY geolocation_zip_code_prefix ORDER BY geolocation_lat, geolocation_row_id) AS latitude_rank,
        ROW_NUMBER() OVER (PARTITION BY geolocation_zip_code_prefix ORDER BY geolocation_lng, geolocation_row_id) AS longitude_rank,
        COUNT(*) OVER (PARTITION BY geolocation_zip_code_prefix) AS sample_count
    FROM {{ ref('stg_geolocation') }}
), coordinates AS (
    SELECT geolocation_zip_code_prefix AS zip_code_prefix,
        TRIM_SCALE((MAX(geolocation_lat) FILTER (WHERE latitude_rank = (sample_count + 1) / 2)
            + MAX(geolocation_lat) FILTER (WHERE latitude_rank = (sample_count + 2) / 2)) * 0.5) AS latitude,
        TRIM_SCALE((MAX(geolocation_lng) FILTER (WHERE longitude_rank = (sample_count + 1) / 2)
            + MAX(geolocation_lng) FILTER (WHERE longitude_rank = (sample_count + 2) / 2)) * 0.5) AS longitude,
        COUNT(*) AS sample_count, MIN(first_seen_at) AS created_at, MAX(last_seen_at) AS updated_at
    FROM ranked GROUP BY geolocation_zip_code_prefix
), labels AS (
    SELECT geolocation_zip_code_prefix AS zip_code_prefix, geolocation_city AS city,
        geolocation_state AS state, COUNT(*) AS label_count,
        ROW_NUMBER() OVER (PARTITION BY geolocation_zip_code_prefix
            ORDER BY COUNT(*) DESC, geolocation_state COLLATE "C", geolocation_city COLLATE "C") AS position
    FROM {{ ref('stg_geolocation') }}
    WHERE geolocation_city IS NOT NULL AND geolocation_state IN (
        'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA',
        'PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO')
    GROUP BY geolocation_zip_code_prefix, geolocation_city, geolocation_state
)
SELECT c.*, l.city, l.state, COALESCE(l.label_count, 0) AS modal_sample_count
FROM coordinates c LEFT JOIN labels l ON c.zip_code_prefix = l.zip_code_prefix AND l.position = 1
