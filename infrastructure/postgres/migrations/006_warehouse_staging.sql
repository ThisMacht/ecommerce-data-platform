-- Version-aware Silver staging; independent of the end-to-end pipeline watermark.

CREATE SCHEMA raw_staging;

REVOKE ALL ON SCHEMA raw_staging FROM PUBLIC;

GRANT USAGE ON SCHEMA raw_staging TO :"source_loader_user", :"source_reader_user";

CREATE TABLE raw_staging.customers (
    customer_id text NOT NULL,
    customer_unique_id text NOT NULL,
    customer_zip_code_prefix text,
    customer_city text,
    customer_state text,
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
    PRIMARY KEY (customer_id)
);

REVOKE ALL ON raw_staging.customers FROM PUBLIC;

GRANT SELECT, INSERT, UPDATE ON raw_staging.customers TO :"source_loader_user";

GRANT SELECT ON raw_staging.customers TO :"source_reader_user";

CREATE TABLE raw_staging.orders (
    order_id text NOT NULL,
    customer_id text NOT NULL,
    order_status text NOT NULL,
    order_purchase_timestamp timestamp with time zone NOT NULL,
    order_approved_at timestamp with time zone,
    order_delivered_carrier_date timestamp with time zone,
    order_delivered_customer_date timestamp with time zone,
    order_estimated_delivery_date timestamp with time zone NOT NULL,
    source_updated_at timestamp with time zone NOT NULL,
    source_sequence bigint NOT NULL,
    source_operation text NOT NULL,
    source_batch_id text NOT NULL,
    source_loaded_at timestamp with time zone NOT NULL,
    order_purchase_date date NOT NULL,
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
    PRIMARY KEY (order_id)
);

REVOKE ALL ON raw_staging.orders FROM PUBLIC;

GRANT SELECT, INSERT, UPDATE ON raw_staging.orders TO :"source_loader_user";

GRANT SELECT ON raw_staging.orders TO :"source_reader_user";

CREATE TABLE raw_staging.order_items (
    order_id text NOT NULL,
    order_item_id integer NOT NULL,
    product_id text NOT NULL,
    seller_id text NOT NULL,
    shipping_limit_date timestamp with time zone NOT NULL,
    price numeric(18,2) NOT NULL,
    freight_value numeric(18,2) NOT NULL,
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
    PRIMARY KEY (order_id, order_item_id)
);

REVOKE ALL ON raw_staging.order_items FROM PUBLIC;

GRANT SELECT, INSERT, UPDATE ON raw_staging.order_items TO :"source_loader_user";

GRANT SELECT ON raw_staging.order_items TO :"source_reader_user";

CREATE TABLE raw_staging.order_payments (
    order_id text NOT NULL,
    payment_sequential integer NOT NULL,
    payment_type text NOT NULL,
    payment_installments integer NOT NULL,
    payment_value numeric(18,2) NOT NULL,
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
    PRIMARY KEY (order_id, payment_sequential)
);

REVOKE ALL ON raw_staging.order_payments FROM PUBLIC;

GRANT SELECT, INSERT, UPDATE ON raw_staging.order_payments TO :"source_loader_user";

GRANT SELECT ON raw_staging.order_payments TO :"source_reader_user";

CREATE TABLE audit.warehouse_load_runs (
    source_table text NOT NULL CHECK (source_table IN ('customers', 'orders', 'order_items', 'order_payments')),
    run_id text NOT NULL CHECK (run_id ~ '^[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}$'),
    request jsonb NOT NULL,
    request_sha256 text NOT NULL CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
    silver_counts jsonb NOT NULL,
    load_candidate_rows bigint NOT NULL CHECK (load_candidate_rows >= 0),
    warehouse_inserted_rows bigint NOT NULL CHECK (warehouse_inserted_rows >= 0),
    warehouse_updated_rows bigint NOT NULL CHECK (warehouse_updated_rows >= 0),
    warehouse_unchanged_rows bigint NOT NULL CHECK (warehouse_unchanged_rows >= 0),
    warehouse_stale_ignored_rows bigint NOT NULL CHECK (warehouse_stale_ignored_rows >= 0),
    committed_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source_table, run_id),
    CHECK (load_candidate_rows = warehouse_inserted_rows + warehouse_updated_rows
        + warehouse_unchanged_rows + warehouse_stale_ignored_rows),
    CHECK (load_candidate_rows = (silver_counts->>'valid_rows')::bigint)
);
REVOKE ALL ON audit.warehouse_load_runs FROM PUBLIC;
GRANT SELECT, INSERT ON audit.warehouse_load_runs TO :"source_loader_user";
GRANT SELECT ON audit.warehouse_load_runs TO :"source_reader_user";
