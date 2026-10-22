-- Independent full product snapshot delivery state; orders state is unchanged.
CREATE TABLE audit.bronze_products_control (
    pipeline text PRIMARY KEY CHECK (pipeline = 'bronze_products_v1'),
    cursor_value jsonb,
    cursor_version bigint NOT NULL DEFAULT 0 CHECK (cursor_version >= 0),
    updated_by_run_id text,
    updated_at timestamp with time zone NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE audit.bronze_products_runs (
    run_id text PRIMARY KEY CHECK (run_id ~ '^[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}$'),
    pipeline text NOT NULL REFERENCES audit.bronze_products_control (pipeline),
    status text NOT NULL CHECK (status IN ('PREPARED', 'COMMITTED')),
    config_sha256 text NOT NULL CHECK (config_sha256 ~ '^[0-9a-f]{64}$'),
    cursor_version_before bigint NOT NULL,
    manifest jsonb NOT NULL,
    prepared_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    committed_at timestamp with time zone,
    CHECK ((status = 'COMMITTED') = (committed_at IS NOT NULL))
);

CREATE UNIQUE INDEX bronze_products_one_prepared_run
    ON audit.bronze_products_runs (pipeline) WHERE status = 'PREPARED';

-- Durable snapshot pages make process-level publication retries reproducible.
CREATE TABLE audit.bronze_products_parts (
    run_id text NOT NULL REFERENCES audit.bronze_products_runs (run_id),
    page_number integer NOT NULL CHECK (page_number > 0),
    body bytea NOT NULL,
    PRIMARY KEY (run_id, page_number)
);

REVOKE ALL ON audit.bronze_products_control, audit.bronze_products_runs,
    audit.bronze_products_parts FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON audit.bronze_products_control,
    audit.bronze_products_runs TO :"source_loader_user";
GRANT SELECT, INSERT ON audit.bronze_products_parts TO :"source_loader_user";
GRANT SELECT ON audit.bronze_products_control, audit.bronze_products_runs
    TO :"source_reader_user";

CREATE TABLE raw_staging.products (
    product_id text NOT NULL,
    product_category_name text,
    product_name_lenght integer CHECK (product_name_lenght >= 0),
    product_description_lenght integer CHECK (product_description_lenght >= 0),
    product_photos_qty integer CHECK (product_photos_qty >= 0),
    product_weight_g integer CHECK (product_weight_g >= 0),
    product_length_cm integer CHECK (product_length_cm >= 0),
    product_height_cm integer CHECK (product_height_cm >= 0),
    product_width_cm integer CHECK (product_width_cm >= 0),
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
    PRIMARY KEY (product_id)
);

REVOKE ALL ON raw_staging.products FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON raw_staging.products TO :"source_loader_user";
GRANT SELECT ON raw_staging.products TO :"source_reader_user";

ALTER TABLE audit.warehouse_load_runs DROP CONSTRAINT warehouse_load_runs_source_table_check;
ALTER TABLE audit.warehouse_load_runs ADD CONSTRAINT warehouse_load_runs_source_table_check
    CHECK (source_table IN ('customers', 'orders', 'order_items', 'order_payments', 'products'));
