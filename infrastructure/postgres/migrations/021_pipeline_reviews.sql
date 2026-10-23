-- Own full review snapshots without adding a pipeline completion cursor.
ALTER TABLE audit.bronze_order_reviews_control DROP CONSTRAINT bronze_order_reviews_control_pipeline_check;
ALTER TABLE audit.bronze_order_reviews_control ADD CONSTRAINT bronze_order_reviews_control_pipeline_check
    CHECK (pipeline IN ('bronze_order_reviews_v1', 'full_order_reviews_snapshot_v1',
        'incremental_order_reviews_parent_v1', 'backfill_order_reviews_parent_v1'));
