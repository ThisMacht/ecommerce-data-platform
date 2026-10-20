"""Full-snapshot source tests using an isolated, disposable PostgreSQL container."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
import unittest
from pathlib import Path
from uuid import uuid4

from olist_platform.raw.contracts import load_contract, validate_contract
from olist_platform.raw.manifest import load_manifest, verify_snapshot
from olist_platform.raw.schema import validate_database_schema

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
EXPECTED_COUNTS = {
    "customers": 99441,
    "geolocation": 1000163,
    "order_items": 112650,
    "order_payments": 103886,
    "order_reviews": 99224,
    "orders": 99441,
    "products": 32951,
    "sellers": 3095,
    "product_category_name_translation": 71,
}


def identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def run(*arguments: str, sql: str | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        arguments, input=sql, capture_output=True, text=True, timeout=240, check=False
    )


def require_success(result: subprocess.CompletedProcess) -> str:
    if result.returncode:
        raise RuntimeError(f"Command failed:\n{result.stdout}\n{result.stderr}")
    return result.stdout.strip()


class SourceLayerIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        raw_dir = Path(
            os.environ.get("OLIST_TEST_RAW_DIR", REPOSITORY_ROOT / "data/raw")
        ).resolve()
        manifest_path = Path(
            os.environ.get(
                "OLIST_TEST_MANIFEST",
                REPOSITORY_ROOT / "config/raw_dataset_manifest.json",
            )
        ).resolve()
        contract_path = Path(
            os.environ.get(
                "OLIST_TEST_CONTRACT", REPOSITORY_ROOT / "config/source_contract.json"
            )
        ).resolve()
        compose_file = Path(
            os.environ.get(
                "OLIST_TEST_COMPOSE_FILE",
                REPOSITORY_ROOT / "infrastructure/docker-compose.yml",
            )
        ).resolve()
        manifest = load_manifest(manifest_path)
        cls.manifest_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        _, errors = verify_snapshot(manifest, raw_dir)
        cls.contract = load_contract(contract_path)
        errors.extend(validate_contract(cls.contract, manifest))
        if errors:
            raise RuntimeError("Source test preflight failed: " + "; ".join(errors))

        configuration = json.loads(
            require_success(
                run(
                    "docker",
                    "compose",
                    "-f",
                    str(compose_file),
                    "config",
                    "--format",
                    "json",
                )
            )
        )
        image = configuration["services"]["postgres"]["image"]
        cls.container = f"olist-source-test-{uuid4().hex[:12]}"
        # Cleanup is restricted to this suite's unique container and anonymous volume.
        cls.addClassCleanup(cls.remove_container)
        arguments = [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            cls.container,
            "--network",
            "none",
            "--memory",
            "2g",
            "--shm-size",
            "256m",
            "--env",
            "POSTGRES_USER=olist_test_admin",
            "--env",
            "POSTGRES_PASSWORD=isolated_test_admin",
            "--env",
            "POSTGRES_DB=postgres",
            "--env",
            "TZ=UTC",
            "--mount",
            "type=volume,destination=/var/lib/postgresql/data",
            "--mount",
            f"type=bind,source={raw_dir},destination=/data/raw,readonly",
            "--mount",
            f"type=bind,source={manifest_path},destination=/config/raw_dataset_manifest.json,readonly",
        ]
        for directory in ["scripts", "migrations", "seeds", "checks"]:
            source = REPOSITORY_ROOT / "infrastructure/postgres" / directory
            arguments.extend(
                [
                    "--mount",
                    f"type=bind,source={source},destination=/{directory},readonly",
                ]
            )
        require_success(run(*arguments, image))
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            result = run(
                "docker",
                "exec",
                cls.container,
                "pg_isready",
                "--host=127.0.0.1",
                "-U",
                "olist_test_admin",
                "-d",
                "postgres",
            )
            if result.returncode == 0:
                break
            time.sleep(0.5)
        else:
            logs = run("docker", "logs", cls.container)
            raise RuntimeError(f"Test PostgreSQL did not become ready:\n{logs.stderr}")
        cls.catalog_sql = (
            REPOSITORY_ROOT / "infrastructure/postgres/checks/export_source_schema.sql"
        ).read_text(encoding="utf-8")

    @classmethod
    def remove_container(cls) -> None:
        result = run("docker", "rm", "--force", "--volumes", cls.container)
        if result.returncode and "No such container" not in result.stderr:
            raise RuntimeError(f"Cannot remove test container: {result.stderr}")

    def setUp(self) -> None:
        self.database = f"source_test_{uuid4().hex[:12]}"
        self.psql(f"CREATE DATABASE {identifier(self.database)};", database="postgres")
        self.addCleanup(self.drop_database)
        require_success(self.job("run_migrations.sh"))

    def drop_database(self) -> None:
        self.psql(
            f"DROP DATABASE {identifier(self.database)} WITH (FORCE);",
            database="postgres",
        )

    def psql(
        self, sql: str, *, user: str = "olist_test_admin", database: str | None = None
    ) -> str:
        return require_success(
            run(
                "docker",
                "exec",
                "--interactive",
                self.container,
                "psql",
                "--no-psqlrc",
                "--set=ON_ERROR_STOP=1",
                "--quiet",
                "--tuples-only",
                "--no-align",
                "--username",
                user,
                "--dbname",
                database or self.database,
                sql=sql,
            )
        )

    def job(self, script: str) -> subprocess.CompletedProcess:
        environment = {
            "PGHOST": "127.0.0.1",
            "PGPORT": "5432",
            "PGOPTIONS": "-c statement_timeout=60000",
            "POSTGRES_DB": self.database,
            "POSTGRES_USER": "olist_test_admin",
            "POSTGRES_PASSWORD": "isolated_test_admin",
            "SOURCE_LOADER_USER": "olist_test_loader",
            "SOURCE_LOADER_PASSWORD": "isolated_test_loader",
            "SOURCE_READER_USER": "olist_test_reader",
            "SOURCE_READER_PASSWORD": "isolated_test_reader",
            "SOURCE_SNAPSHOT_ID": "olist-kaggle-local-v1",
            "SOURCE_SEED_BATCH_ID": "seed_olist_kaggle_local_v1",
            "SOURCE_SEED_TIMESTAMP": "2020-01-01T00:00:00Z",
        }
        arguments = ["docker", "exec"]
        for key, value in environment.items():
            arguments.extend(["--env", f"{key}={value}"])
        return run(*arguments, self.container, "/bin/sh", f"/scripts/{script}")

    def catalog(self, mutation: str = "") -> dict:
        sql = self.catalog_sql
        if mutation:
            sql = f"BEGIN; {mutation}\n{sql}\nROLLBACK;"
        return json.loads(
            self.psql(
                sql, user="olist_test_reader" if not mutation else "olist_test_admin"
            )
        )

    def snapshot(self) -> dict:
        """Hash every field, including operational metadata, in primary-key order."""
        tables = {}
        for table in self.contract["tables"]:
            name = table["table"]
            qualified = f"olist.{identifier(name)}"
            order = ", ".join(
                f"record.{identifier(key)}" for key in table["primary_key"]
            )
            state = json.loads(
                self.psql(f"""
                SET TIME ZONE 'UTC';
                SELECT jsonb_build_object(
                    'rows', count(*),
                    'sha256', encode(sha256(convert_to(COALESCE(string_agg(
                        encode(sha256(convert_to(to_jsonb(record)::text, 'UTF8')), 'hex'),
                        '' ORDER BY {order}
                    ), ''), 'UTF8')), 'hex')
                ) FROM {qualified} record;
            """)
            )
            sequence = self.psql(
                f"SELECT pg_get_serial_sequence({literal(qualified)}, 'source_sequence');"
            )
            state["sequence"] = json.loads(
                self.psql(
                    f"SELECT jsonb_build_object('last_value', last_value, 'is_called', is_called) FROM {sequence};"
                )
            )
            tables[name] = state
        audit = json.loads(
            self.psql(
                "SET TIME ZONE 'UTC'; SELECT COALESCE(jsonb_agg(to_jsonb(record) ORDER BY snapshot_id), '[]') FROM audit.source_seed_runs record;"
            )
        )
        migrations = json.loads(
            self.psql(
                "SET TIME ZONE 'UTC'; SELECT jsonb_agg(to_jsonb(record) ORDER BY version) FROM audit.schema_migrations record;"
            )
        )
        return {"tables": tables, "audit": audit, "migrations": migrations}

    def assert_seeded(self) -> None:
        counts = {}
        for name in EXPECTED_COUNTS:
            counts[name] = int(
                self.psql(f"SELECT count(*) FROM olist.{identifier(name)};")
            )
        self.assertEqual(counts, EXPECTED_COUNTS)
        self.assertEqual(
            self.psql(
                "SELECT snapshot_id || '|' || status FROM audit.source_seed_runs;"
            ),
            "olist-kaggle-local-v1|SUCCESS",
        )
        self.psql("\\i /checks/verify_olist_source.sql", user="olist_test_reader")

    def test_live_schema_matches_contract_and_detects_drift(self) -> None:
        self.assertEqual(
            validate_database_schema(self.contract, self.catalog()), ([], [])
        )
        before = self.psql(
            "SELECT checksum || '|' || applied_at::text FROM audit.schema_migrations;"
        )
        require_success(self.job("run_migrations.sh"))
        self.assertEqual(
            before,
            self.psql(
                "SELECT checksum || '|' || applied_at::text FROM audit.schema_migrations;"
            ),
        )
        cases = [
            (
                "ALTER TABLE olist.order_items ALTER COLUMN price TYPE numeric(20,4);",
                "Type mismatch",
            ),
            (
                "ALTER TABLE olist.orders ALTER COLUMN order_purchase_timestamp DROP NOT NULL;",
                "Nullability mismatch",
            ),
            (
                "ALTER TABLE olist.order_items DROP CONSTRAINT order_items_primary_key, ADD PRIMARY KEY (order_item_id, order_id);",
                "Primary-key mismatch",
            ),
            (
                "ALTER TABLE olist.customers DROP COLUMN source_operation;",
                "Missing column",
            ),
            (
                "ALTER TABLE olist.customers ALTER COLUMN source_sequence DROP IDENTITY;",
                "BY DEFAULT identity",
            ),
            (
                "ALTER TABLE olist.customers DROP CONSTRAINT customers_source_sequence_unique;",
                "unique constraint",
            ),
        ]
        for mutation, message in cases:
            with self.subTest(mutation=mutation):
                errors, _ = validate_database_schema(
                    self.contract, self.catalog(mutation)
                )
                self.assertTrue(any(message in error for error in errors), errors)

    def test_seed_rerun_preserves_all_data_metadata_and_sequences(self) -> None:
        require_success(self.job("run_source_seed.sh"))
        self.assert_seeded()
        before = self.snapshot()
        result = self.job("run_source_seed.sh")
        self.assertIn("already loaded (no-op)", require_success(result))
        self.assertEqual(before, self.snapshot())

        # Emulate a later source update; no simulator implementation is assumed.
        self.psql("""
            UPDATE olist.customers SET
                customer_city = 'integration-update', source_sequence = DEFAULT,
                source_updated_at = '2021-01-01T00:00:00Z',
                source_operation = 'UPDATE', source_batch_id = 'integration-update'
            WHERE customer_id = (SELECT min(customer_id) FROM olist.customers);
        """)
        updated = self.snapshot()
        self.assertNotEqual(
            before["tables"]["customers"], updated["tables"]["customers"]
        )
        self.assertIn(
            "already loaded (no-op)", require_success(self.job("run_source_seed.sh"))
        )
        self.assertEqual(updated, self.snapshot())

        # Exercise SQL ON CONFLICT behavior too, bypassing only the shell no-op.
        self.psql(
            f"""
            \\set snapshot_id 'olist-kaggle-local-v1'
            \\set manifest_sha256 '{self.manifest_sha256}'
            \\set seed_batch_id 'seed_olist_kaggle_local_v1'
            \\set seed_source_updated_at '2020-01-01T00:00:00Z'
            \\i /seeds/001_seed_olist_source.sql
        """,
            user="olist_test_loader",
        )
        self.assertEqual(updated, self.snapshot())

    def install_failure(self, table: str, message: str) -> None:
        self.psql(f"""
            CREATE FUNCTION public.reject_test_seed() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM olist.customers) THEN
                    RAISE EXCEPTION 'Failure did not follow partial source writes';
                END IF;
                RAISE EXCEPTION {literal(message)};
            END;
            $$;
            CREATE TRIGGER reject_test_seed BEFORE INSERT ON {table}
                FOR EACH STATEMENT EXECUTE FUNCTION public.reject_test_seed();
        """)

    def remove_failure(self, table: str) -> None:
        self.psql(
            f"DROP TRIGGER reject_test_seed ON {table}; DROP FUNCTION public.reject_test_seed();"
        )

    def test_failed_late_table_insert_rolls_back_and_can_retry(self) -> None:
        before = self.snapshot()
        table = "olist.product_category_name_translation"
        # Suppress the last table's inserts, so the production presence check
        # must detect missing keys after the other source tables were written.
        self.psql(f"""
            CREATE FUNCTION public.reject_test_seed() RETURNS trigger LANGUAGE plpgsql
                AS $$ BEGIN RETURN NULL; END; $$;
            CREATE TRIGGER reject_test_seed BEFORE INSERT ON {table}
                FOR EACH ROW EXECUTE FUNCTION public.reject_test_seed();
        """)
        result = self.job("run_source_seed.sh")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Not every seeded translation key exists", result.stderr)
        self.assertIn("INSERT 0 99441", result.stdout)
        self.assertEqual(before, self.snapshot())
        self.remove_failure(table)
        require_success(self.job("run_source_seed.sh"))
        self.assert_seeded()

    def test_failed_audit_insert_rolls_back_rows_and_can_retry(self) -> None:
        before = self.snapshot()
        table = "audit.source_seed_runs"
        self.install_failure(table, "injected audit failure")
        result = self.job("run_source_seed.sh")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("injected audit failure", result.stderr)
        after = self.snapshot()
        # PostgreSQL setval is nontransactional; failed seeds may leave sequence
        # gaps. Source rows, their metadata, and audit rows must still roll back.
        for state in [before, after]:
            for values in state["tables"].values():
                values.pop("sequence")
        self.assertEqual(before, after)
        self.remove_failure(table)
        require_success(self.job("run_source_seed.sh"))
        self.assert_seeded()


if __name__ == "__main__":
    unittest.main()
