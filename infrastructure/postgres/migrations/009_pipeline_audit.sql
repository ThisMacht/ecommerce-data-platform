-- Local full-snapshot pipeline state, distinct from Bronze delivery checkpoints.
CREATE TABLE audit.pipeline_control (
    pipeline_name text NOT NULL CHECK (pipeline_name = 'local_core_v1'),
    source_table text NOT NULL CHECK (source_table IN ('customers','orders','order_items','order_payments')),
    committed_cursor jsonb,
    cursor_version bigint NOT NULL DEFAULT 0 CHECK (cursor_version >= 0),
    updated_by_run_id text,
    updated_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (pipeline_name, source_table)
);
CREATE TABLE audit.pipeline_runs (
    run_id text PRIMARY KEY CHECK (run_id ~ '^[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}$'),
    pipeline_name text NOT NULL CHECK (pipeline_name = 'local_core_v1'),
    run_type text NOT NULL DEFAULT 'manual' CHECK (run_type = 'manual'),
    request jsonb NOT NULL,
    request_sha256 text NOT NULL,
    control_before jsonb NOT NULL,
    attempt integer NOT NULL DEFAULT 1 CHECK (attempt > 0),
    status text NOT NULL CHECK (status IN ('RUNNING','FAILED','SUCCESS')),
    started_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamp with time zone,
    failed_task text,
    result jsonb,
    CHECK ((status = 'RUNNING') = (completed_at IS NULL))
);
CREATE UNIQUE INDEX pipeline_one_running ON audit.pipeline_runs (pipeline_name) WHERE status='RUNNING';
CREATE TABLE audit.pipeline_task_runs (
    run_id text NOT NULL REFERENCES audit.pipeline_runs(run_id),
    run_attempt integer NOT NULL CHECK (run_attempt > 0),
    task_name text NOT NULL,
    status text NOT NULL CHECK (status IN ('RUNNING','FAILED','SUCCESS')),
    started_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamp with time zone,
    PRIMARY KEY (run_id, run_attempt, task_name),
    CHECK ((status = 'RUNNING') = (completed_at IS NULL))
);
CREATE TABLE audit.pipeline_dataset_runs (
    run_id text NOT NULL REFERENCES audit.pipeline_runs(run_id),
    source_table text NOT NULL,
    evidence jsonb NOT NULL,
    counters jsonb NOT NULL,
    watermark_before jsonb,
    watermark_candidate jsonb,
    watermark_after jsonb,
    reconciliation_status text NOT NULL CHECK (reconciliation_status='PASS'),
    PRIMARY KEY (run_id,source_table)
);
REVOKE ALL ON audit.pipeline_control, audit.pipeline_runs, audit.pipeline_task_runs,
    audit.pipeline_dataset_runs FROM PUBLIC;
GRANT SELECT,INSERT,UPDATE ON audit.pipeline_control, audit.pipeline_runs,
    audit.pipeline_task_runs TO :"source_loader_user";
GRANT SELECT,INSERT ON audit.pipeline_dataset_runs TO :"source_loader_user";
GRANT SELECT ON audit.pipeline_control, audit.pipeline_runs, audit.pipeline_task_runs,
    audit.pipeline_dataset_runs TO :"source_reader_user";
