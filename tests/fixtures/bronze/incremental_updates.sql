-- Independently specified: baseline cursor Jan 10, inclusive 48h lower bound Jan 8.
UPDATE olist.customers SET customer_city='Campinas', source_sequence=100,
    source_updated_at='2020-01-09T00:00:00Z', source_operation='UPDATE' WHERE customer_id='customer_a';
UPDATE olist.orders SET order_status='approved', source_sequence=100,
    source_updated_at='2020-01-09T00:00:00Z', source_operation='UPDATE' WHERE order_id='order_a';
UPDATE olist.order_items SET price=22.50, source_sequence=100,
    source_updated_at='2020-01-09T00:00:00Z', source_operation='UPDATE' WHERE order_id='order_a' AND order_item_id=1;
UPDATE olist.order_payments SET payment_value=33.00, source_sequence=100,
    source_updated_at='2020-01-09T00:00:00Z', source_operation='UPDATE' WHERE order_id='order_a' AND payment_sequential=1;

INSERT INTO olist.customers (customer_id,customer_unique_id,customer_zip_code_prefix,
    customer_city,customer_state,source_sequence,source_updated_at,source_operation,source_batch_id)
VALUES
    ('customer_boundary','shopper_boundary','00123','Sao Paulo','SP',101,'2020-01-08T00:00:00Z','INSERT','late_fixture'),
    ('customer_outside','shopper_outside','00123','Sao Paulo','SP',102,'2020-01-07T23:59:59Z','INSERT','late_fixture'),
    ('customer_new','shopper_new','00123','Sao Paulo','SP',103,'2020-01-11T00:00:00Z','INSERT','late_fixture');
INSERT INTO olist.orders (order_id,customer_id,order_status,order_purchase_timestamp,
    order_estimated_delivery_date,source_sequence,source_updated_at,source_operation,source_batch_id)
VALUES
    ('order_boundary','customer_boundary','created','2018-01-01 12:00:00','2018-01-10',101,'2020-01-08T00:00:00Z','INSERT','late_fixture'),
    ('order_outside','customer_outside','created','2018-01-01 12:00:00','2018-01-10',102,'2020-01-07T23:59:59Z','INSERT','late_fixture'),
    ('order_new','customer_old','created','2018-01-01 12:00:00','2018-01-10',103,'2020-01-11T00:00:00Z','INSERT','late_fixture');
INSERT INTO olist.order_items (order_id,order_item_id,product_id,seller_id,shipping_limit_date,
    price,freight_value,source_sequence,source_updated_at,source_operation,source_batch_id)
VALUES
    ('order_boundary',1,'product','seller','2018-01-01 12:00:00',4.00,0.00,101,'2020-01-08T00:00:00Z','INSERT','late_fixture'),
    ('order_outside',1,'product','seller','2018-01-01 12:00:00',5.00,0.00,102,'2020-01-07T23:59:59Z','INSERT','late_fixture'),
    ('order_new',1,'product','seller','2018-01-01 12:00:00',6.00,0.00,103,'2020-01-11T00:00:00Z','INSERT','late_fixture'),
    ('order_old',1,'product','seller','2018-01-01 12:00:00',7.00,0.00,104,'2020-01-11T00:00:00Z','INSERT','late_fixture');
INSERT INTO olist.order_payments (order_id,payment_sequential,payment_type,payment_installments,
    payment_value,source_sequence,source_updated_at,source_operation,source_batch_id)
VALUES
    ('order_boundary',1,'credit_card',1,4.00,101,'2020-01-08T00:00:00Z','INSERT','late_fixture'),
    ('order_outside',1,'credit_card',1,5.00,102,'2020-01-07T23:59:59Z','INSERT','late_fixture'),
    ('order_new',1,'credit_card',1,6.00,103,'2020-01-11T00:00:00Z','INSERT','late_fixture'),
    ('order_old',1,'credit_card',1,7.00,104,'2020-01-11T00:00:00Z','INSERT','late_fixture');
