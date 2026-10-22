-- Own geolocation full snapshots without adding a pipeline completion cursor.
ALTER TABLE audit.bronze_geolocation_control DROP CONSTRAINT bronze_geolocation_control_pipeline_check;
ALTER TABLE audit.bronze_geolocation_control ADD CONSTRAINT bronze_geolocation_control_pipeline_check
    CHECK (pipeline IN ('bronze_geolocation_v1', 'full_geolocation_snapshot_v1',
        'incremental_geolocation_parent_v1', 'backfill_geolocation_parent_v1'));
