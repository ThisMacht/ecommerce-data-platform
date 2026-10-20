-- customer_b deliberately arrives later; shopper-level IDs may repeat.
INSERT INTO olist.customers (
    customer_id, customer_unique_id, customer_zip_code_prefix, customer_city,
    customer_state, source_updated_at, source_operation, source_batch_id
) VALUES (
    'customer_a', 'shared_shopper', '00123', ' São Paulo ', 'sp',
    '2020-01-04T00:00:00Z', 'INSERT', 'customer_fixture'
);
