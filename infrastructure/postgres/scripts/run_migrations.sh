#!/bin/sh
set -eu

: "${PGHOST:?PGHOST is required}"
: "${PGPORT:?PGPORT is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"
: "${SOURCE_LOADER_USER:?SOURCE_LOADER_USER is required}"
: "${SOURCE_LOADER_PASSWORD:?SOURCE_LOADER_PASSWORD is required}"
: "${SOURCE_READER_USER:?SOURCE_READER_USER is required}"
: "${SOURCE_READER_PASSWORD:?SOURCE_READER_PASSWORD is required}"

case "${SOURCE_LOADER_USER}" in
    ""|[0-9]*|*[!a-z0-9_]*)
        echo "Invalid SOURCE_LOADER_USER: ${SOURCE_LOADER_USER}" >&2
        exit 2
        ;;
esac

case "${SOURCE_READER_USER}" in
    ""|[0-9]*|*[!a-z0-9_]*)
        echo "Invalid SOURCE_READER_USER: ${SOURCE_READER_USER}" >&2
        exit 2
        ;;
esac

export PGPASSWORD="${POSTGRES_PASSWORD}"

psql_admin() {
    psql \
        --host="${PGHOST}" \
        --port="${PGPORT}" \
        --username="${POSTGRES_USER}" \
        --dbname="${POSTGRES_DB}" \
        --no-psqlrc \
        --set=ON_ERROR_STOP=1 \
        "$@"
}

for migration_path in /migrations/*.sql; do
    migration_filename="$(basename "${migration_path}")"
    case "${migration_filename}" in
        [0-9][0-9][0-9]_[a-z0-9_]*.sql) ;;
        *) echo "Invalid migration filename: ${migration_filename}" >&2; exit 2 ;;
    esac

    migration_version="${migration_filename%%_*}"
    migration_checksum="$(sha256sum "${migration_path}" | cut -d ' ' -f 1)"
    migrations_table_exists="$(
        psql_admin --tuples-only --no-align \
            --command="SELECT to_regclass('audit.schema_migrations') IS NOT NULL;"
    )"

    applied_checksum=""
    if [ "${migrations_table_exists}" = "t" ]; then
        applied_checksum="$(
            psql_admin --tuples-only --no-align \
                --command="SELECT checksum FROM audit.schema_migrations WHERE version = '${migration_version}';"
        )"
    fi

    if [ -n "${applied_checksum}" ]; then
        if [ "${applied_checksum}" != "${migration_checksum}" ]; then
            echo "Migration ${migration_filename} changed after it was applied." >&2
            echo "Applied checksum: ${applied_checksum}" >&2
            echo "Current checksum: ${migration_checksum}" >&2
            exit 1
        fi
        echo "Migration ${migration_filename}: already applied"
        continue
    fi

    echo "Migration ${migration_filename}: applying"
    psql_admin \
        --single-transaction \
        --set=database_name="${POSTGRES_DB}" \
        --set=source_loader_user="${SOURCE_LOADER_USER}" \
        --set=source_loader_password="${SOURCE_LOADER_PASSWORD}" \
        --set=source_reader_user="${SOURCE_READER_USER}" \
        --set=source_reader_password="${SOURCE_READER_PASSWORD}" \
        --file="${migration_path}" \
        --command="INSERT INTO audit.schema_migrations (version, filename, checksum) VALUES ('${migration_version}', '${migration_filename}', '${migration_checksum}');"
done

echo "Database migrations: PASS"

# TODO(phase-0): Add an advisory migration lock before parallel CI workers share
# one PostgreSQL instance. Local Compose currently has a single migration writer.
