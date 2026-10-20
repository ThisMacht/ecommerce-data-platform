-- Small synthetic baseline: only the two references needed by fixture batches.
-- IDs match references in the canonical Olist snapshot; attributes are synthetic.
INSERT INTO olist.products (
    product_id, source_updated_at, source_operation, source_batch_id
) VALUES (
    '4244733e06e7ecb4970a6e2683c13e61', '2020-01-01T00:00:00Z', 'INSERT', 'test_references'
);
INSERT INTO olist.sellers (
    seller_id, seller_zip_code_prefix, seller_city, seller_state,
    source_updated_at, source_operation, source_batch_id
) VALUES (
    '48436dade18ac8b2bce089ec2a041202', '01000', 'fixture city', 'SP',
    '2020-01-01T00:00:00Z', 'INSERT', 'test_references'
);
