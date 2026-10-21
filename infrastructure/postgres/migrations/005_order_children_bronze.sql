-- Independent composite-key Bronze delivery state; no end-to-end watermark promotion.
CREATE TABLE audit.bronze_order_payments_control (
    pipeline text PRIMARY KEY CHECK (pipeline = 'bronze_order_payments_v1'),
    cursor_value jsonb,
    cursor_version bigint NOT NULL DEFAULT 0 CHECK (cursor_version >= 0),
    updated_by_run_id text,
    updated_at timestamp with time zone NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE audit.bronze_order_payments_runs (
    run_id text PRIMARY KEY CHECK (run_id ~ '^[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}$'),
    pipeline text NOT NULL REFERENCES audit.bronze_order_payments_control (pipeline),
    status text NOT NULL CHECK (status IN ('PREPARED', 'COMMITTED')),
    config_sha256 text NOT NULL CHECK (config_sha256 ~ '^[0-9a-f]{64}$'),
    cursor_version_before bigint NOT NULL,
    manifest jsonb NOT NULL,
    prepared_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    committed_at timestamp with time zone,
    CHECK ((status = 'COMMITTED') = (committed_at IS NOT NULL))
);

CREATE UNIQUE INDEX bronze_order_payments_one_prepared_run
    ON audit.bronze_order_payments_runs (pipeline) WHERE status = 'PREPARED';

-- Durable snapshot pages make process-level publication retries reproducible.
CREATE TABLE audit.bronze_order_payments_parts (
    run_id text NOT NULL REFERENCES audit.bronze_order_payments_runs (run_id),
    page_number integer NOT NULL CHECK (page_number > 0),
    body bytea NOT NULL,
    PRIMARY KEY (run_id, page_number)
);

REVOKE ALL ON audit.bronze_order_payments_control, audit.bronze_order_payments_runs,
    audit.bronze_order_payments_parts FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON audit.bronze_order_payments_control,
    audit.bronze_order_payments_runs TO :"source_loader_user";
GRANT SELECT, INSERT ON audit.bronze_order_payments_parts TO :"source_loader_user";
GRANT SELECT ON audit.bronze_order_payments_control, audit.bronze_order_payments_runs
    TO :"source_reader_user";

CREATE TABLE audit.bronze_order_items_control (
    pipeline text PRIMARY KEY CHECK (pipeline = 'bronze_order_items_v1'),
    cursor_value jsonb,
    cursor_version bigint NOT NULL DEFAULT 0 CHECK (cursor_version >= 0),
    updated_by_run_id text,
    updated_at timestamp with time zone NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE audit.bronze_order_items_runs (
    run_id text PRIMARY KEY CHECK (run_id ~ '^[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}$'),
    pipeline text NOT NULL REFERENCES audit.bronze_order_items_control (pipeline),
    status text NOT NULL CHECK (status IN ('PREPARED', 'COMMITTED')),
    config_sha256 text NOT NULL CHECK (config_sha256 ~ '^[0-9a-f]{64}$'),
    cursor_version_before bigint NOT NULL,
    manifest jsonb NOT NULL,
    prepared_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    committed_at timestamp with time zone,
    CHECK ((status = 'COMMITTED') = (committed_at IS NOT NULL))
);

CREATE UNIQUE INDEX bronze_order_items_one_prepared_run
    ON audit.bronze_order_items_runs (pipeline) WHERE status = 'PREPARED';

-- Durable snapshot pages make process-level publication retries reproducible.
CREATE TABLE audit.bronze_order_items_parts (
    run_id text NOT NULL REFERENCES audit.bronze_order_items_runs (run_id),
    page_number integer NOT NULL CHECK (page_number > 0),
    body bytea NOT NULL,
    PRIMARY KEY (run_id, page_number)
);

REVOKE ALL ON audit.bronze_order_items_control, audit.bronze_order_items_runs,
    audit.bronze_order_items_parts FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON audit.bronze_order_items_control,
    audit.bronze_order_items_runs TO :"source_loader_user";
GRANT SELECT, INSERT ON audit.bronze_order_items_parts TO :"source_loader_user";
GRANT SELECT ON audit.bronze_order_items_control, audit.bronze_order_items_runs
    TO :"source_reader_user";
