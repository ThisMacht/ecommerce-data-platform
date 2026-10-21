-- Shared deferred quarantine. Original Silver artifacts remain immutable.
CREATE TABLE audit.quarantine_records (
    record_uid text NOT NULL CHECK (record_uid ~ '^[0-9a-f]{64}$'),
    rule_id text NOT NULL CHECK (rule_id IN ('missing_customer', 'missing_order')),
    logical_record_uid text NOT NULL,
    run_id text NOT NULL,
    source_table text NOT NULL CHECK (source_table IN ('orders', 'order_items', 'order_payments')),
    source_primary_key jsonb NOT NULL,
    source_version bigint NOT NULL CHECK (source_version > 0),
    severity text NOT NULL DEFAULT 'DEFERRED' CHECK (severity = 'DEFERRED'),
    raw_payload jsonb NOT NULL,
    payload jsonb NOT NULL,
    payload_sha256 text NOT NULL,
    contract_sha256 text NOT NULL,
    origin_silver_prefix text NOT NULL,
    origin_manifest_sha256 text NOT NULL,
    first_seen_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    last_attempt_at timestamp with time zone,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 100),
    max_deferred_age_hours integer NOT NULL CHECK (max_deferred_age_hours BETWEEN 1 AND 8760),
    retry_status text NOT NULL DEFAULT 'PENDING' CHECK (retry_status IN ('PENDING', 'RESOLVED', 'EXPIRED')),
    resolved_at timestamp with time zone,
    terminal_reason text,
    PRIMARY KEY (record_uid, rule_id),
    CHECK ((retry_status = 'RESOLVED') = (resolved_at IS NOT NULL)),
    CHECK ((retry_status = 'EXPIRED') = (terminal_reason IS NOT NULL))
);
CREATE INDEX quarantine_pending ON audit.quarantine_records (source_table, first_seen_at, record_uid)
    WHERE retry_status = 'PENDING';

CREATE TABLE audit.deferred_registration_runs (
    source_table text NOT NULL,
    run_id text NOT NULL,
    request_sha256 text NOT NULL,
    request jsonb NOT NULL,
    input_rows bigint NOT NULL CHECK (input_rows >= 0),
    inserted_rows bigint NOT NULL CHECK (inserted_rows BETWEEN 0 AND input_rows),
    committed_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source_table, run_id)
);
CREATE TABLE audit.deferred_resolution_runs (
    source_table text NOT NULL,
    run_id text NOT NULL,
    request_sha256 text NOT NULL,
    request jsonb NOT NULL,
    result jsonb NOT NULL,
    committed_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source_table, run_id)
);
CREATE TABLE audit.deferred_attempts (
    source_table text NOT NULL,
    run_id text NOT NULL,
    record_uid text NOT NULL,
    rule_id text NOT NULL,
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    outcome text NOT NULL CHECK (outcome IN ('PENDING', 'RESOLVED', 'EXPIRED')),
    terminal_reason text,
    PRIMARY KEY (source_table, run_id, record_uid, rule_id),
    FOREIGN KEY (record_uid, rule_id) REFERENCES audit.quarantine_records (record_uid, rule_id),
    FOREIGN KEY (source_table, run_id) REFERENCES audit.deferred_resolution_runs (source_table, run_id)
        DEFERRABLE INITIALLY DEFERRED
);
REVOKE ALL ON audit.quarantine_records, audit.deferred_registration_runs,
    audit.deferred_resolution_runs, audit.deferred_attempts FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON audit.quarantine_records TO :"source_loader_user";
GRANT SELECT, INSERT ON audit.deferred_registration_runs, audit.deferred_resolution_runs,
    audit.deferred_attempts TO :"source_loader_user";
GRANT SELECT ON audit.quarantine_records, audit.deferred_registration_runs,
    audit.deferred_resolution_runs, audit.deferred_attempts TO :"source_reader_user";
