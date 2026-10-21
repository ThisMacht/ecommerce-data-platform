-- Incremental completion state stays independent of full-run and delivery cursors.
ALTER TABLE audit.pipeline_control DROP CONSTRAINT pipeline_control_pipeline_name_check;
ALTER TABLE audit.pipeline_control ADD CHECK (pipeline_name IN ('local_core_v1', 'incremental_core_v1'));
ALTER TABLE audit.pipeline_runs DROP CONSTRAINT pipeline_runs_pipeline_name_check;
ALTER TABLE audit.pipeline_runs ADD CHECK (pipeline_name IN ('local_core_v1', 'incremental_core_v1'));
ALTER TABLE audit.pipeline_runs DROP CONSTRAINT pipeline_runs_run_type_check;
ALTER TABLE audit.pipeline_runs ADD CHECK (run_type IN ('manual', 'incremental'));
DROP INDEX audit.pipeline_one_running;
CREATE UNIQUE INDEX pipeline_one_running ON audit.pipeline_runs ((true)) WHERE status='RUNNING';

-- Separate delivery streams let full FK snapshots and deltas freeze together.
ALTER TABLE audit.bronze_customers_control DROP CONSTRAINT bronze_customers_control_pipeline_check;
ALTER TABLE audit.bronze_customers_control ADD CHECK (pipeline IN (
    'bronze_customers_v1', 'incremental_customers_delta_v1', 'incremental_customers_parent_v1'));
ALTER TABLE audit.bronze_orders_control DROP CONSTRAINT bronze_orders_control_pipeline_check;
ALTER TABLE audit.bronze_orders_control ADD CHECK (pipeline IN (
    'bronze_orders_v1', 'incremental_orders_delta_v1', 'incremental_orders_parent_v1'));
ALTER TABLE audit.bronze_order_items_control DROP CONSTRAINT bronze_order_items_control_pipeline_check;
ALTER TABLE audit.bronze_order_items_control ADD CHECK (pipeline IN (
    'bronze_order_items_v1', 'incremental_order_items_delta_v1'));
ALTER TABLE audit.bronze_order_payments_control DROP CONSTRAINT bronze_order_payments_control_pipeline_check;
ALTER TABLE audit.bronze_order_payments_control ADD CHECK (pipeline IN (
    'bronze_order_payments_v1', 'incremental_order_payments_delta_v1'));
