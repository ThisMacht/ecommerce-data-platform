#!/bin/sh
set -eu

: "${PGHOST:?PGHOST is required}"
: "${PGPORT:?PGPORT is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${SOURCE_LOADER_USER:?SOURCE_LOADER_USER is required}"
: "${SOURCE_LOADER_PASSWORD:?SOURCE_LOADER_PASSWORD is required}"
: "${SOURCE_SNAPSHOT_ID:?SOURCE_SNAPSHOT_ID is required}"
: "${SOURCE_SEED_BATCH_ID:?SOURCE_SEED_BATCH_ID is required}"
: "${SOURCE_SEED_TIMESTAMP:?SOURCE_SEED_TIMESTAMP is required}"

manifest_path="/config/raw_dataset_manifest.json"
if [ ! -r "${manifest_path}" ]; then
    echo "Raw manifest is not readable: ${manifest_path}" >&2
    exit 2
fi

manifest_sha256="$(sha256sum "${manifest_path}" | cut -d ' ' -f 1)"
export PGPASSWORD="${SOURCE_LOADER_PASSWORD}"

psql_loader() {
    psql \
        --host="${PGHOST}" \
        --port="${PGPORT}" \
        --username="${SOURCE_LOADER_USER}" \
        --dbname="${POSTGRES_DB}" \
        --no-psqlrc \
        --set=ON_ERROR_STOP=1 \
        "$@"
}

if ! existing_seed="$(
    psql_loader \
        --tuples-only \
        --no-align \
        --set=snapshot_id="${SOURCE_SNAPSHOT_ID}" \
        --file=/scripts/get_source_seed.sql
)"; then
    echo "Source schema is unavailable; run migrations first." >&2
    exit 1
fi

if [ -n "${existing_seed}" ]; then
    expected_seed="${manifest_sha256}|SUCCESS"
    if [ "${existing_seed}" = "${expected_seed}" ]; then
        echo "Source seed ${SOURCE_SNAPSHOT_ID}: already loaded (no-op)"
        exit 0
    fi

    echo "Source seed ${SOURCE_SNAPSHOT_ID} already exists with different metadata." >&2
    echo "Existing: ${existing_seed}" >&2
    echo "Expected: ${expected_seed}" >&2
    exit 1
fi

psql_loader \
    --set=snapshot_id="${SOURCE_SNAPSHOT_ID}" \
    --set=manifest_sha256="${manifest_sha256}" \
    --set=seed_batch_id="${SOURCE_SEED_BATCH_ID}" \
    --set=seed_source_updated_at="${SOURCE_SEED_TIMESTAMP}" \
    --file=/seeds/001_seed_olist_source.sql

echo "Source seed ${SOURCE_SNAPSHOT_ID}: PASS"

# TODO(phase-0): Direct `docker compose run source-loader` verifies the manifest
# checksum only. `make seed-source` remains authoritative because it first runs
# the logical CSV/header/file checksum verifier on the host.
