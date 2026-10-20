-- Idempotent seed of the verified Olist raw snapshot.
-- Existing source keys are never overwritten, preserving later simulator updates.

BEGIN;

SET LOCAL TIME ZONE 'UTC';
SELECT pg_advisory_xact_lock(hashtext('olist.seed.olist-kaggle-local-v1'));

CREATE TEMP TABLE stage_customers (
    source_row_number bigint GENERATED ALWAYS AS IDENTITY,
    customer_id text,
    customer_unique_id text,
    customer_zip_code_prefix text,
    customer_city text,
    customer_state text
) ON COMMIT DROP;

CREATE TEMP TABLE stage_geolocation (
    source_row_number bigint GENERATED ALWAYS AS IDENTITY,
    geolocation_zip_code_prefix text,
    geolocation_lat text,
    geolocation_lng text,
    geolocation_city text,
    geolocation_state text
) ON COMMIT DROP;

CREATE TEMP TABLE stage_order_items (
    source_row_number bigint GENERATED ALWAYS AS IDENTITY,
    order_id text,
    order_item_id text,
    product_id text,
    seller_id text,
    shipping_limit_date text,
    price text,
    freight_value text
) ON COMMIT DROP;

CREATE TEMP TABLE stage_order_payments (
    source_row_number bigint GENERATED ALWAYS AS IDENTITY,
    order_id text,
    payment_sequential text,
    payment_type text,
    payment_installments text,
    payment_value text
) ON COMMIT DROP;

CREATE TEMP TABLE stage_order_reviews (
    source_row_number bigint GENERATED ALWAYS AS IDENTITY,
    review_id text,
    order_id text,
    review_score text,
    review_comment_title text,
    review_comment_message text,
    review_creation_date text,
    review_answer_timestamp text
) ON COMMIT DROP;

CREATE TEMP TABLE stage_orders (
    source_row_number bigint GENERATED ALWAYS AS IDENTITY,
    order_id text,
    customer_id text,
    order_status text,
    order_purchase_timestamp text,
    order_approved_at text,
    order_delivered_carrier_date text,
    order_delivered_customer_date text,
    order_estimated_delivery_date text
) ON COMMIT DROP;

CREATE TEMP TABLE stage_products (
    source_row_number bigint GENERATED ALWAYS AS IDENTITY,
    product_id text,
    product_category_name text,
    product_name_lenght text,
    product_description_lenght text,
    product_photos_qty text,
    product_weight_g text,
    product_length_cm text,
    product_height_cm text,
    product_width_cm text
) ON COMMIT DROP;

CREATE TEMP TABLE stage_sellers (
    source_row_number bigint GENERATED ALWAYS AS IDENTITY,
    seller_id text,
    seller_zip_code_prefix text,
    seller_city text,
    seller_state text
) ON COMMIT DROP;

CREATE TEMP TABLE stage_product_category_translation (
    source_row_number bigint GENERATED ALWAYS AS IDENTITY,
    product_category_name text,
    product_category_name_english text
) ON COMMIT DROP;

\copy stage_customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state) FROM '/data/raw/olist_customers_dataset.csv' WITH (FORMAT CSV, HEADER true, ENCODING 'UTF8')
\copy stage_geolocation (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state) FROM '/data/raw/olist_geolocation_dataset.csv' WITH (FORMAT CSV, HEADER true, ENCODING 'UTF8')
\copy stage_order_items (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value) FROM '/data/raw/olist_order_items_dataset.csv' WITH (FORMAT CSV, HEADER true, ENCODING 'UTF8')
\copy stage_order_payments (order_id, payment_sequential, payment_type, payment_installments, payment_value) FROM '/data/raw/olist_order_payments_dataset.csv' WITH (FORMAT CSV, HEADER true, ENCODING 'UTF8')
\copy stage_order_reviews (review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp) FROM '/data/raw/olist_order_reviews_dataset.csv' WITH (FORMAT CSV, HEADER true, ENCODING 'UTF8')
\copy stage_orders (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date) FROM '/data/raw/olist_orders_dataset.csv' WITH (FORMAT CSV, HEADER true, ENCODING 'UTF8')
\copy stage_products (product_id, product_category_name, product_name_lenght, product_description_lenght, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm) FROM '/data/raw/olist_products_dataset.csv' WITH (FORMAT CSV, HEADER true, ENCODING 'UTF8')
\copy stage_sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state) FROM '/data/raw/olist_sellers_dataset.csv' WITH (FORMAT CSV, HEADER true, ENCODING 'UTF8')
\copy stage_product_category_translation (product_category_name, product_category_name_english) FROM '/data/raw/product_category_name_translation.csv' WITH (FORMAT CSV, HEADER true, ENCODING 'UTF8')

-- COPY into temporary tables does not automatically create planner statistics.
-- These indexes plus ANALYZE keep relationship checks from degrading into large
-- nested-loop joins on a fresh database.
CREATE UNIQUE INDEX stage_customers_key_idx
    ON stage_customers (customer_id);
CREATE UNIQUE INDEX stage_order_items_key_idx
    ON stage_order_items (order_id, order_item_id);
CREATE INDEX stage_order_items_product_idx
    ON stage_order_items (product_id);
CREATE INDEX stage_order_items_seller_idx
    ON stage_order_items (seller_id);
CREATE UNIQUE INDEX stage_order_payments_key_idx
    ON stage_order_payments (order_id, payment_sequential);
CREATE UNIQUE INDEX stage_order_reviews_key_idx
    ON stage_order_reviews (review_id, order_id);
CREATE INDEX stage_order_reviews_order_idx
    ON stage_order_reviews (order_id);
CREATE UNIQUE INDEX stage_orders_key_idx
    ON stage_orders (order_id);
CREATE INDEX stage_orders_customer_idx
    ON stage_orders (customer_id);
CREATE UNIQUE INDEX stage_products_key_idx
    ON stage_products (product_id);
CREATE UNIQUE INDEX stage_sellers_key_idx
    ON stage_sellers (seller_id);
CREATE UNIQUE INDEX stage_product_category_translation_key_idx
    ON stage_product_category_translation (product_category_name);

ANALYZE stage_customers;
ANALYZE stage_geolocation;
ANALYZE stage_order_items;
ANALYZE stage_order_payments;
ANALYZE stage_order_reviews;
ANALYZE stage_orders;
ANALYZE stage_products;
ANALYZE stage_sellers;
ANALYZE stage_product_category_translation;

DO $validation$
DECLARE
    count_mismatches text;
BEGIN
    SELECT string_agg(
        format('%s expected=%s actual=%s', table_name, expected_count, actual_count),
        '; ' ORDER BY table_name
    )
    INTO count_mismatches
    FROM (
        VALUES
            ('customers', 99441::bigint, (SELECT count(*) FROM stage_customers)),
            ('geolocation', 1000163::bigint, (SELECT count(*) FROM stage_geolocation)),
            ('order_items', 112650::bigint, (SELECT count(*) FROM stage_order_items)),
            ('order_payments', 103886::bigint, (SELECT count(*) FROM stage_order_payments)),
            ('order_reviews', 99224::bigint, (SELECT count(*) FROM stage_order_reviews)),
            ('orders', 99441::bigint, (SELECT count(*) FROM stage_orders)),
            ('products', 32951::bigint, (SELECT count(*) FROM stage_products)),
            ('sellers', 3095::bigint, (SELECT count(*) FROM stage_sellers)),
            ('product_category_name_translation', 71::bigint, (SELECT count(*) FROM stage_product_category_translation))
    ) AS counts(table_name, expected_count, actual_count)
    WHERE expected_count <> actual_count;

    IF count_mismatches IS NOT NULL THEN
        RAISE EXCEPTION 'Raw row-count validation failed: %', count_mismatches;
    END IF;

    IF EXISTS (
        SELECT 1 FROM stage_customers
        WHERE NULLIF(customer_id, '') IS NULL
        GROUP BY customer_id HAVING count(*) > 0
    ) OR EXISTS (
        SELECT 1 FROM stage_customers
        GROUP BY customer_id HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'customers source key is null, empty or duplicated';
    END IF;

    IF EXISTS (
        SELECT 1 FROM stage_order_items
        WHERE NULLIF(order_id, '') IS NULL OR NULLIF(order_item_id, '') IS NULL
    ) OR EXISTS (
        SELECT 1 FROM stage_order_items
        GROUP BY order_id, order_item_id HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'order_items source key is null, empty or duplicated';
    END IF;

    IF EXISTS (
        SELECT 1 FROM stage_order_payments
        WHERE NULLIF(order_id, '') IS NULL OR NULLIF(payment_sequential, '') IS NULL
    ) OR EXISTS (
        SELECT 1 FROM stage_order_payments
        GROUP BY order_id, payment_sequential HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'order_payments source key is null, empty or duplicated';
    END IF;

    IF EXISTS (
        SELECT 1 FROM stage_order_reviews
        WHERE NULLIF(review_id, '') IS NULL OR NULLIF(order_id, '') IS NULL
    ) OR EXISTS (
        SELECT 1 FROM stage_order_reviews
        GROUP BY review_id, order_id HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'order_reviews source key is null, empty or duplicated';
    END IF;

    IF EXISTS (
        SELECT 1 FROM stage_orders WHERE NULLIF(order_id, '') IS NULL
    ) OR EXISTS (
        SELECT 1 FROM stage_orders GROUP BY order_id HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'orders source key is null, empty or duplicated';
    END IF;

    IF EXISTS (
        SELECT 1 FROM stage_products WHERE NULLIF(product_id, '') IS NULL
    ) OR EXISTS (
        SELECT 1 FROM stage_products GROUP BY product_id HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'products source key is null, empty or duplicated';
    END IF;

    IF EXISTS (
        SELECT 1 FROM stage_sellers WHERE NULLIF(seller_id, '') IS NULL
    ) OR EXISTS (
        SELECT 1 FROM stage_sellers GROUP BY seller_id HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'sellers source key is null, empty or duplicated';
    END IF;

    IF EXISTS (
        SELECT 1 FROM stage_product_category_translation
        WHERE NULLIF(product_category_name, '') IS NULL
    ) OR EXISTS (
        SELECT 1 FROM stage_product_category_translation
        GROUP BY product_category_name HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'product translation source key is null, empty or duplicated';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM stage_orders child
        LEFT JOIN stage_customers parent USING (customer_id)
        WHERE parent.customer_id IS NULL
    ) THEN
        RAISE EXCEPTION 'orders contains customer_id values absent from customers';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM stage_order_items child
        LEFT JOIN stage_orders parent USING (order_id)
        WHERE parent.order_id IS NULL
    ) THEN
        RAISE EXCEPTION 'order_items contains order_id values absent from orders';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM stage_order_items child
        LEFT JOIN stage_products parent USING (product_id)
        WHERE parent.product_id IS NULL
    ) THEN
        RAISE EXCEPTION 'order_items contains product_id values absent from products';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM stage_order_items child
        LEFT JOIN stage_sellers parent USING (seller_id)
        WHERE parent.seller_id IS NULL
    ) THEN
        RAISE EXCEPTION 'order_items contains seller_id values absent from sellers';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM stage_order_payments child
        LEFT JOIN stage_orders parent USING (order_id)
        WHERE parent.order_id IS NULL
    ) THEN
        RAISE EXCEPTION 'order_payments contains order_id values absent from orders';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM stage_order_reviews child
        LEFT JOIN stage_orders parent USING (order_id)
        WHERE parent.order_id IS NULL
    ) THEN
        RAISE EXCEPTION 'order_reviews contains order_id values absent from orders';
    END IF;
END
$validation$;

INSERT INTO olist.customers (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    source_updated_at,
    source_sequence,
    source_operation,
    source_batch_id
)
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    :'seed_source_updated_at'::timestamp with time zone,
    source_row_number,
    'INSERT',
    :'seed_batch_id'
FROM stage_customers
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO olist.geolocation (
    geolocation_row_id,
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state,
    source_updated_at,
    source_sequence,
    source_operation,
    source_batch_id
)
SELECT
    source_row_number,
    geolocation_zip_code_prefix,
    geolocation_lat::numeric,
    geolocation_lng::numeric,
    geolocation_city,
    geolocation_state,
    :'seed_source_updated_at'::timestamp with time zone,
    source_row_number,
    'INSERT',
    :'seed_batch_id'
FROM stage_geolocation
ON CONFLICT (geolocation_row_id) DO NOTHING;

INSERT INTO olist.order_items (
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    source_updated_at,
    source_sequence,
    source_operation,
    source_batch_id
)
SELECT
    order_id,
    order_item_id::integer,
    product_id,
    seller_id,
    shipping_limit_date::timestamp without time zone,
    price::numeric(18, 2),
    freight_value::numeric(18, 2),
    :'seed_source_updated_at'::timestamp with time zone,
    source_row_number,
    'INSERT',
    :'seed_batch_id'
FROM stage_order_items
ON CONFLICT (order_id, order_item_id) DO NOTHING;

INSERT INTO olist.order_payments (
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    source_updated_at,
    source_sequence,
    source_operation,
    source_batch_id
)
SELECT
    order_id,
    payment_sequential::integer,
    payment_type,
    payment_installments::integer,
    payment_value::numeric(18, 2),
    :'seed_source_updated_at'::timestamp with time zone,
    source_row_number,
    'INSERT',
    :'seed_batch_id'
FROM stage_order_payments
ON CONFLICT (order_id, payment_sequential) DO NOTHING;

INSERT INTO olist.order_reviews (
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,
    source_updated_at,
    source_sequence,
    source_operation,
    source_batch_id
)
SELECT
    review_id,
    order_id,
    review_score::integer,
    NULLIF(review_comment_title, ''),
    NULLIF(review_comment_message, ''),
    review_creation_date::timestamp without time zone,
    review_answer_timestamp::timestamp without time zone,
    :'seed_source_updated_at'::timestamp with time zone,
    source_row_number,
    'INSERT',
    :'seed_batch_id'
FROM stage_order_reviews
ON CONFLICT (review_id, order_id) DO NOTHING;

INSERT INTO olist.orders (
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    source_updated_at,
    source_sequence,
    source_operation,
    source_batch_id
)
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp::timestamp without time zone,
    NULLIF(order_approved_at, '')::timestamp without time zone,
    NULLIF(order_delivered_carrier_date, '')::timestamp without time zone,
    NULLIF(order_delivered_customer_date, '')::timestamp without time zone,
    order_estimated_delivery_date::timestamp without time zone,
    :'seed_source_updated_at'::timestamp with time zone,
    source_row_number,
    'INSERT',
    :'seed_batch_id'
FROM stage_orders
ON CONFLICT (order_id) DO NOTHING;

INSERT INTO olist.products (
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    source_updated_at,
    source_sequence,
    source_operation,
    source_batch_id
)
SELECT
    product_id,
    NULLIF(product_category_name, ''),
    NULLIF(product_name_lenght, '')::integer,
    NULLIF(product_description_lenght, '')::integer,
    NULLIF(product_photos_qty, '')::integer,
    NULLIF(product_weight_g, '')::integer,
    NULLIF(product_length_cm, '')::integer,
    NULLIF(product_height_cm, '')::integer,
    NULLIF(product_width_cm, '')::integer,
    :'seed_source_updated_at'::timestamp with time zone,
    source_row_number,
    'INSERT',
    :'seed_batch_id'
FROM stage_products
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO olist.sellers (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    source_updated_at,
    source_sequence,
    source_operation,
    source_batch_id
)
SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    :'seed_source_updated_at'::timestamp with time zone,
    source_row_number,
    'INSERT',
    :'seed_batch_id'
FROM stage_sellers
ON CONFLICT (seller_id) DO NOTHING;

INSERT INTO olist.product_category_name_translation (
    product_category_name,
    product_category_name_english,
    source_updated_at,
    source_sequence,
    source_operation,
    source_batch_id
)
SELECT
    product_category_name,
    product_category_name_english,
    :'seed_source_updated_at'::timestamp with time zone,
    source_row_number,
    'INSERT',
    :'seed_batch_id'
FROM stage_product_category_translation
ON CONFLICT (product_category_name) DO NOTHING;

DO $presence_validation$
BEGIN
    IF EXISTS (
        SELECT 1 FROM stage_customers source
        LEFT JOIN olist.customers target USING (customer_id)
        WHERE target.customer_id IS NULL
    ) THEN
        RAISE EXCEPTION 'Not every seeded customer key exists in the target';
    END IF;

    IF EXISTS (
        SELECT 1 FROM stage_geolocation source
        LEFT JOIN olist.geolocation target
            ON target.geolocation_row_id = source.source_row_number
        WHERE target.geolocation_row_id IS NULL
    ) THEN
        RAISE EXCEPTION 'Not every seeded geolocation row exists in the target';
    END IF;

    IF EXISTS (
        SELECT 1 FROM stage_order_items source
        LEFT JOIN olist.order_items target
            ON target.order_id = source.order_id
           AND target.order_item_id = source.order_item_id::integer
        WHERE target.order_id IS NULL
    ) THEN
        RAISE EXCEPTION 'Not every seeded order-item key exists in the target';
    END IF;

    IF EXISTS (
        SELECT 1 FROM stage_order_payments source
        LEFT JOIN olist.order_payments target
            ON target.order_id = source.order_id
           AND target.payment_sequential = source.payment_sequential::integer
        WHERE target.order_id IS NULL
    ) THEN
        RAISE EXCEPTION 'Not every seeded payment key exists in the target';
    END IF;

    IF EXISTS (
        SELECT 1 FROM stage_order_reviews source
        LEFT JOIN olist.order_reviews target USING (review_id, order_id)
        WHERE target.review_id IS NULL
    ) THEN
        RAISE EXCEPTION 'Not every seeded review key exists in the target';
    END IF;

    IF EXISTS (
        SELECT 1 FROM stage_orders source
        LEFT JOIN olist.orders target USING (order_id)
        WHERE target.order_id IS NULL
    ) THEN
        RAISE EXCEPTION 'Not every seeded order key exists in the target';
    END IF;

    IF EXISTS (
        SELECT 1 FROM stage_products source
        LEFT JOIN olist.products target USING (product_id)
        WHERE target.product_id IS NULL
    ) THEN
        RAISE EXCEPTION 'Not every seeded product key exists in the target';
    END IF;

    IF EXISTS (
        SELECT 1 FROM stage_sellers source
        LEFT JOIN olist.sellers target USING (seller_id)
        WHERE target.seller_id IS NULL
    ) THEN
        RAISE EXCEPTION 'Not every seeded seller key exists in the target';
    END IF;

    IF EXISTS (
        SELECT 1 FROM stage_product_category_translation source
        LEFT JOIN olist.product_category_name_translation target
            USING (product_category_name)
        WHERE target.product_category_name IS NULL
    ) THEN
        RAISE EXCEPTION 'Not every seeded translation key exists in the target';
    END IF;
END
$presence_validation$;

SELECT setval(
    pg_get_serial_sequence('olist.customers', 'source_sequence'),
    (SELECT max(source_sequence) FROM olist.customers),
    true
);
SELECT setval(
    pg_get_serial_sequence('olist.geolocation', 'source_sequence'),
    (SELECT max(source_sequence) FROM olist.geolocation),
    true
);
SELECT setval(
    pg_get_serial_sequence('olist.order_items', 'source_sequence'),
    (SELECT max(source_sequence) FROM olist.order_items),
    true
);
SELECT setval(
    pg_get_serial_sequence('olist.order_payments', 'source_sequence'),
    (SELECT max(source_sequence) FROM olist.order_payments),
    true
);
SELECT setval(
    pg_get_serial_sequence('olist.order_reviews', 'source_sequence'),
    (SELECT max(source_sequence) FROM olist.order_reviews),
    true
);
SELECT setval(
    pg_get_serial_sequence('olist.orders', 'source_sequence'),
    (SELECT max(source_sequence) FROM olist.orders),
    true
);
SELECT setval(
    pg_get_serial_sequence('olist.products', 'source_sequence'),
    (SELECT max(source_sequence) FROM olist.products),
    true
);
SELECT setval(
    pg_get_serial_sequence('olist.sellers', 'source_sequence'),
    (SELECT max(source_sequence) FROM olist.sellers),
    true
);
SELECT setval(
    pg_get_serial_sequence(
        'olist.product_category_name_translation',
        'source_sequence'
    ),
    (SELECT max(source_sequence)
     FROM olist.product_category_name_translation),
    true
);

INSERT INTO audit.source_seed_runs (
    snapshot_id,
    manifest_sha256,
    seed_batch_id,
    seed_source_updated_at,
    row_counts,
    status
)
VALUES (
    :'snapshot_id',
    :'manifest_sha256',
    :'seed_batch_id',
    :'seed_source_updated_at'::timestamp with time zone,
    jsonb_build_object(
        'customers', 99441,
        'geolocation', 1000163,
        'order_items', 112650,
        'order_payments', 103886,
        'order_reviews', 99224,
        'orders', 99441,
        'products', 32951,
        'sellers', 3095,
        'product_category_name_translation', 71
    ),
    'SUCCESS'
)
ON CONFLICT (snapshot_id) DO NOTHING;

COMMIT;

-- TODO(phase-0): Replace snapshot-specific COPY statements with a generated,
-- contract-driven loader when the source simulator application image is added.
