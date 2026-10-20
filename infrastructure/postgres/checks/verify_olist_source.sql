\pset pager off

DO $verification$
DECLARE
    count_mismatches text;
BEGIN
    SELECT string_agg(
        format('%s expected-at-least=%s actual=%s', table_name, expected_count, actual_count),
        '; ' ORDER BY table_name
    )
    INTO count_mismatches
    FROM (
        VALUES
            ('customers', 99441::bigint, (SELECT count(*) FROM olist.customers)),
            ('geolocation', 1000163::bigint, (SELECT count(*) FROM olist.geolocation)),
            ('order_items', 112650::bigint, (SELECT count(*) FROM olist.order_items)),
            ('order_payments', 103886::bigint, (SELECT count(*) FROM olist.order_payments)),
            ('order_reviews', 99224::bigint, (SELECT count(*) FROM olist.order_reviews)),
            ('orders', 99441::bigint, (SELECT count(*) FROM olist.orders)),
            ('products', 32951::bigint, (SELECT count(*) FROM olist.products)),
            ('sellers', 3095::bigint, (SELECT count(*) FROM olist.sellers)),
            ('product_category_name_translation', 71::bigint, (SELECT count(*) FROM olist.product_category_name_translation))
    ) AS counts(table_name, expected_count, actual_count)
    WHERE actual_count < expected_count;

    IF count_mismatches IS NOT NULL THEN
        RAISE EXCEPTION 'Source row-count verification failed: %', count_mismatches;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM audit.source_seed_runs
        WHERE snapshot_id = 'olist-kaggle-local-v1'
          AND status = 'SUCCESS'
    ) THEN
        RAISE EXCEPTION 'Successful source seed audit row is missing';
    END IF;

    IF EXISTS (
        SELECT source_sequence
        FROM olist.customers
        GROUP BY source_sequence HAVING count(*) > 1
    ) OR EXISTS (
        SELECT source_sequence
        FROM olist.geolocation
        GROUP BY source_sequence HAVING count(*) > 1
    ) OR EXISTS (
        SELECT source_sequence
        FROM olist.order_items
        GROUP BY source_sequence HAVING count(*) > 1
    ) OR EXISTS (
        SELECT source_sequence
        FROM olist.order_payments
        GROUP BY source_sequence HAVING count(*) > 1
    ) OR EXISTS (
        SELECT source_sequence
        FROM olist.order_reviews
        GROUP BY source_sequence HAVING count(*) > 1
    ) OR EXISTS (
        SELECT source_sequence
        FROM olist.orders
        GROUP BY source_sequence HAVING count(*) > 1
    ) OR EXISTS (
        SELECT source_sequence
        FROM olist.products
        GROUP BY source_sequence HAVING count(*) > 1
    ) OR EXISTS (
        SELECT source_sequence
        FROM olist.sellers
        GROUP BY source_sequence HAVING count(*) > 1
    ) OR EXISTS (
        SELECT source_sequence
        FROM olist.product_category_name_translation
        GROUP BY source_sequence HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'A source_sequence is duplicated within a source table';
    END IF;
END
$verification$;

SELECT 'customers' AS table_name, count(*) AS row_count FROM olist.customers
UNION ALL
SELECT 'geolocation', count(*) FROM olist.geolocation
UNION ALL
SELECT 'order_items', count(*) FROM olist.order_items
UNION ALL
SELECT 'order_payments', count(*) FROM olist.order_payments
UNION ALL
SELECT 'order_reviews', count(*) FROM olist.order_reviews
UNION ALL
SELECT 'orders', count(*) FROM olist.orders
UNION ALL
SELECT 'products', count(*) FROM olist.products
UNION ALL
SELECT 'sellers', count(*) FROM olist.sellers
UNION ALL
SELECT 'product_category_name_translation', count(*)
FROM olist.product_category_name_translation
ORDER BY table_name;
