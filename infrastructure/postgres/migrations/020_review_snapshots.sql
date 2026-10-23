-- Standalone full reviews with composite source grain and pinned-order deferred routing.
CREATE TABLE audit.bronze_order_reviews_control (
    pipeline text PRIMARY KEY CHECK (pipeline = 'bronze_order_reviews_v1'),
    cursor_value jsonb,
    cursor_version bigint NOT NULL DEFAULT 0 CHECK (cursor_version >= 0),
    updated_by_run_id text,
    updated_at timestamp with time zone NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE audit.bronze_order_reviews_runs (
    run_id text PRIMARY KEY CHECK (run_id ~ '^[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}$'),
    pipeline text NOT NULL REFERENCES audit.bronze_order_reviews_control (pipeline),
    status text NOT NULL CHECK (status IN ('PREPARED', 'COMMITTED')),
    config_sha256 text NOT NULL CHECK (config_sha256 ~ '^[0-9a-f]{64}$'),
    cursor_version_before bigint NOT NULL,
    manifest jsonb NOT NULL,
    prepared_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    committed_at timestamp with time zone,
    CHECK ((status = 'COMMITTED') = (committed_at IS NOT NULL))
);

CREATE UNIQUE INDEX bronze_order_reviews_one_prepared_run
    ON audit.bronze_order_reviews_runs (pipeline) WHERE status = 'PREPARED';

-- Durable snapshot pages make process-level publication retries reproducible.
CREATE TABLE audit.bronze_order_reviews_parts (
    run_id text NOT NULL REFERENCES audit.bronze_order_reviews_runs (run_id),
    page_number integer NOT NULL CHECK (page_number > 0),
    body bytea NOT NULL,
    PRIMARY KEY (run_id, page_number)
);

REVOKE ALL ON audit.bronze_order_reviews_control, audit.bronze_order_reviews_runs,
    audit.bronze_order_reviews_parts FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON audit.bronze_order_reviews_control,
    audit.bronze_order_reviews_runs TO :"source_loader_user";
GRANT SELECT, INSERT ON audit.bronze_order_reviews_parts TO :"source_loader_user";
GRANT SELECT ON audit.bronze_order_reviews_control, audit.bronze_order_reviews_runs
    TO :"source_reader_user";

CREATE TABLE raw_staging.order_reviews (
    review_id text NOT NULL CHECK (BTRIM(review_id) <> ''),
    order_id text NOT NULL CHECK (BTRIM(order_id) <> ''),
    review_score integer NOT NULL CHECK (review_score BETWEEN 1 AND 5),
    review_comment_title text,
    review_comment_message text,
    review_creation_date timestamp with time zone NOT NULL,
    review_answer_timestamp timestamp with time zone NOT NULL,
    source_updated_at timestamp with time zone NOT NULL,
    source_sequence bigint NOT NULL,
    source_operation text NOT NULL,
    source_batch_id text NOT NULL,
    source_loaded_at timestamp with time zone NOT NULL,
    _record_uid text NOT NULL,
    _logical_record_uid text NOT NULL,
    _ingested_at timestamp with time zone NOT NULL,
    _run_id text NOT NULL,
    _run_attempt bigint NOT NULL,
    _source_name text NOT NULL,
    _source_file text NOT NULL,
    _source_row_number bigint NOT NULL,
    _schema_version text NOT NULL,
    warning_rule_ids text[] NOT NULL,
    record_hash text NOT NULL CHECK (record_hash ~ '^[0-9a-f]{64}$'),
    hash_schema_version text NOT NULL CHECK (hash_schema_version = 'business-v1'),
    first_seen_run_id text NOT NULL,
    last_seen_run_id text NOT NULL,
    first_seen_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    last_seen_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    CHECK (source_sequence > 0),
    CHECK (source_operation IN ('INSERT', 'UPDATE')),
    PRIMARY KEY (review_id, order_id)
);

REVOKE ALL ON raw_staging.order_reviews FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON raw_staging.order_reviews TO :"source_loader_user";
GRANT SELECT ON raw_staging.order_reviews TO :"source_reader_user";

ALTER TABLE audit.warehouse_load_runs DROP CONSTRAINT warehouse_load_runs_source_table_check;
ALTER TABLE audit.warehouse_load_runs ADD CONSTRAINT warehouse_load_runs_source_table_check
    CHECK (source_table IN ('customers', 'orders', 'order_items', 'order_payments',
        'products', 'sellers', 'geolocation', 'order_reviews'));

ALTER TABLE audit.quarantine_records DROP CONSTRAINT quarantine_records_source_table_check;
ALTER TABLE audit.quarantine_records ADD CONSTRAINT quarantine_records_source_table_check
    CHECK (source_table IN ('orders', 'order_items', 'order_payments', 'order_reviews'));
