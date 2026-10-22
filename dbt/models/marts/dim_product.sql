SELECT
    MD5(product_id) AS product_key,
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    first_seen_at AS created_at,
    last_seen_at AS updated_at,
    record_hash
FROM {{ ref('stg_products') }}
