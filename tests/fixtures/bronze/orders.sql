-- Independent two-order baseline; no canonical CSV snapshot is claimed.
INSERT INTO olist.orders (
    order_id, customer_id, order_status, order_purchase_timestamp,
    order_estimated_delivery_date, source_updated_at, source_operation, source_batch_id
) VALUES
    ('order_a', 'customer_a', 'created', '2018-01-01 12:00:00',
     '2018-01-10', '2020-01-04T00:00:00Z', 'INSERT', 'bronze_fixture'),
    ('order_b', 'customer_b', 'approved', '2018-01-02 12:00:00',
     '2018-01-11', '2020-01-04T00:00:00Z', 'INSERT', 'bronze_fixture');
