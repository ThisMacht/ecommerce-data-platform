-- Successful simulator mutations and their audit evidence commit together.
CREATE TABLE audit.source_batch_runs (
    batch_id text PRIMARY KEY,
    payload_sha256 char(64) NOT NULL,
    contract_sha256 char(64) NOT NULL,
    predecessor_batch_id text REFERENCES audit.source_batch_runs (batch_id),
    applied_at timestamp with time zone NOT NULL,
    operation_count integer NOT NULL CHECK (operation_count > 0),
    operation_counts jsonb NOT NULL CHECK (jsonb_typeof(operation_counts) = 'object'),
    operation_results jsonb NOT NULL CHECK (jsonb_typeof(operation_results) = 'array'),
    committed_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    status text NOT NULL DEFAULT 'SUCCESS' CHECK (status = 'SUCCESS'),
    CONSTRAINT source_batch_payload_sha256_format
        CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT source_batch_contract_sha256_format
        CHECK (contract_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT source_batch_results_count
        CHECK (jsonb_array_length(operation_results) = operation_count),
    CONSTRAINT source_batch_predecessor_differs
        CHECK (predecessor_batch_id IS DISTINCT FROM batch_id)
);

REVOKE ALL ON audit.source_batch_runs FROM PUBLIC;
GRANT SELECT, INSERT ON audit.source_batch_runs TO :"source_loader_user";
GRANT SELECT ON audit.source_batch_runs TO :"source_reader_user";
