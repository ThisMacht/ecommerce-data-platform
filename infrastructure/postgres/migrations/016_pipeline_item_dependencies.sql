-- Owned full snapshots and product/seller FK streams; keep four-table cursor CAS.
ALTER TABLE audit.bronze_customers_control DROP CONSTRAINT bronze_customers_control_pipeline_check;
ALTER TABLE audit.bronze_customers_control ADD CONSTRAINT bronze_customers_control_pipeline_check
    CHECK (pipeline IN ('bronze_customers_v1', 'full_customers_snapshot_v1', 'incremental_customers_delta_v1', 'incremental_customers_parent_v1', 'backfill_customers_delta_v1', 'backfill_customers_parent_v1'));
ALTER TABLE audit.bronze_orders_control DROP CONSTRAINT bronze_orders_control_pipeline_check;
ALTER TABLE audit.bronze_orders_control ADD CONSTRAINT bronze_orders_control_pipeline_check
    CHECK (pipeline IN ('bronze_orders_v1', 'full_orders_snapshot_v1', 'incremental_orders_delta_v1', 'incremental_orders_parent_v1', 'backfill_orders_delta_v1', 'backfill_orders_parent_v1'));
ALTER TABLE audit.bronze_order_items_control DROP CONSTRAINT bronze_order_items_control_pipeline_check;
ALTER TABLE audit.bronze_order_items_control ADD CONSTRAINT bronze_order_items_control_pipeline_check
    CHECK (pipeline IN ('bronze_order_items_v1', 'full_order_items_snapshot_v1', 'incremental_order_items_delta_v1', 'backfill_order_items_delta_v1'));
ALTER TABLE audit.bronze_order_payments_control DROP CONSTRAINT bronze_order_payments_control_pipeline_check;
ALTER TABLE audit.bronze_order_payments_control ADD CONSTRAINT bronze_order_payments_control_pipeline_check
    CHECK (pipeline IN ('bronze_order_payments_v1', 'full_order_payments_snapshot_v1', 'incremental_order_payments_delta_v1', 'backfill_order_payments_delta_v1'));
ALTER TABLE audit.bronze_products_control DROP CONSTRAINT bronze_products_control_pipeline_check;
ALTER TABLE audit.bronze_products_control ADD CONSTRAINT bronze_products_control_pipeline_check
    CHECK (pipeline IN ('bronze_products_v1', 'full_products_snapshot_v1', 'incremental_products_parent_v1', 'backfill_products_parent_v1'));
ALTER TABLE audit.bronze_sellers_control DROP CONSTRAINT bronze_sellers_control_pipeline_check;
ALTER TABLE audit.bronze_sellers_control ADD CONSTRAINT bronze_sellers_control_pipeline_check
    CHECK (pipeline IN ('bronze_sellers_v1', 'full_sellers_snapshot_v1', 'incremental_sellers_parent_v1', 'backfill_sellers_parent_v1'));
