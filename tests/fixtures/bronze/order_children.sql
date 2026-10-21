-- Independent composite-grain inputs; source deliberately has no enforced FK.
INSERT INTO olist.order_payments (
    order_id, payment_sequential, payment_type, payment_installments, payment_value,
    source_updated_at, source_operation, source_batch_id
) VALUES
    ('order_a', 1, 'credit_card', 2, 10.50, '2020-01-04T00:00:00Z', 'INSERT', 'child_fixture'),
    ('order_a', 2, 'voucher', 1, 1.25, '2020-01-04T00:00:00Z', 'INSERT', 'child_fixture'),
    ('late_order', 1, 'boleto', 1, 20.00, '2020-01-04T00:00:00Z', 'INSERT', 'child_fixture'),
    ('invalid_order', 1, 'cash', 1, -1.00, '2020-01-04T00:00:00Z', 'INSERT', 'child_fixture');

INSERT INTO olist.order_items (
    order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value,
    source_updated_at, source_operation, source_batch_id
) VALUES
    ('order_a', 1, 'product', 'seller', '2018-01-01 12:00:00', 10.50, 1.25, '2020-01-04T00:00:00Z', 'INSERT', 'child_fixture'),
    ('order_a', 2, 'product', 'seller', '2018-01-01 12:00:00', 5.00, 0.00, '2020-01-04T00:00:00Z', 'INSERT', 'child_fixture'),
    ('late_order', 1, 'product', 'seller', '2018-01-01 12:00:00', 20.00, 0.00, '2020-01-04T00:00:00Z', 'INSERT', 'child_fixture'),
    ('invalid_order', 1, '', 'seller', '2018-01-01 12:00:00', -1.00, 0.00, '2020-01-04T00:00:00Z', 'INSERT', 'child_fixture');
