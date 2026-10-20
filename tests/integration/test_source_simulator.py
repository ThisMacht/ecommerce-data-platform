from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from postgres_case import REPOSITORY_ROOT, PostgresIntegrationCase
from psycopg import sql
from psycopg.conninfo import make_conninfo

from olist_platform.source_simulator.apply import apply_batch
from olist_platform.source_simulator.batches import CORE_TABLES, BatchError, write_batch
from olist_platform.source_simulator.fixtures import (
    fixture_batches,
    fixture_ids,
    write_fixture,
)


class SourceSimulatorIntegrationTest(PostgresIntegrationCase):
    requires_raw_snapshot = False
    publish_port = True

    def setUp(self) -> None:
        super().setUp()
        self.contract = deepcopy(self.contract)
        self.contract["dataset_snapshot_id"] = "olist-simulator-reference-fixture-v1"
        reference_path = (
            REPOSITORY_ROOT / "tests/fixtures/source_simulator/references.sql"
        )
        self.psql(reference_path.read_text())
        reference_hash = hashlib.sha256(reference_path.read_bytes()).hexdigest()
        self.psql(f"""
            INSERT INTO audit.source_seed_runs (
                snapshot_id, manifest_sha256, seed_batch_id,
                seed_source_updated_at, row_counts, status
            ) VALUES (
                'olist-simulator-reference-fixture-v1', '{reference_hash}',
                'test_references', '2020-01-01T00:00:00Z',
                '{{"customers":0,"orders":0,"order_items":0,"order_payments":0,
                  "products":1,"sellers":1,"geolocation":0,"order_reviews":0,
                  "product_category_name_translation":0}}'::jsonb,
                'SUCCESS'
            );
        """)
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.batches = write_fixture(self.directory / "fixture", self.contract)
        self.ids = fixture_ids()

    def connection(self, role: str = "loader") -> psycopg.Connection:
        return psycopg.connect(
            host="127.0.0.1",
            port=self.port,
            dbname=self.database,
            user=f"olist_test_{role}",
            password=f"isolated_test_{role}",
            autocommit=True,
            connect_timeout=10,
            application_name="simulator-integration",
        )

    def state(self, *, sequences: bool = True) -> dict:
        result = {}
        with self.connection() as connection:
            for table in self.contract["tables"]:
                name = table["table"]
                if name not in CORE_TABLES:
                    continue
                order = sql.SQL(", ").join(map(sql.Identifier, table["primary_key"]))
                result[name] = connection.execute(
                    sql.SQL(
                        "SELECT COALESCE(jsonb_agg(to_jsonb(record) ORDER BY {}), '[]') FROM {} record"
                    ).format(order, sql.Identifier("olist", name))
                ).fetchone()[0]
                if sequences:
                    sequence = connection.execute(
                        "SELECT pg_get_serial_sequence(%s, 'source_sequence')",
                        (f"olist.{name}",),
                    ).fetchone()[0]
                    result[name + "_sequence"] = json.loads(
                        self.psql(
                            f"SELECT jsonb_build_object('last_value', last_value, 'is_called', is_called) FROM {sequence};"
                        )
                    )
            result["audit"] = connection.execute(
                "SELECT COALESCE(jsonb_agg(to_jsonb(record) ORDER BY batch_id), '[]') FROM audit.source_batch_runs record"
            ).fetchone()[0]
        return result

    def custom_batch(
        self, name: str, operations: list[dict], predecessor: str | None = None
    ) -> Path:
        path = self.directory / name
        payload = {
            "format_version": 1,
            "batch_id": name,
            "predecessor_batch_id": predecessor,
            "applied_at": "2020-01-06T00:00:00Z",
            "operations": operations,
        }
        write_batch(path, payload, self.contract, {})
        return path

    def apply_initial(self) -> None:
        with self.connection() as connection:
            self.assertEqual(
                apply_batch(connection, self.batches[0], self.contract)["status"],
                "APPLIED",
            )

    def test_four_batches_versions_late_parents_and_replays(self) -> None:
        expected_counts = [(2, 2, 2, 1), (2, 2, 3, 2), (3, 3, 3, 3), (3, 3, 3, 3)]
        with self.connection() as connection:
            for index, path in enumerate(self.batches):
                self.assertEqual(
                    apply_batch(connection, path, self.contract)["status"], "APPLIED"
                )
                counts = tuple(
                    connection.execute(
                        sql.SQL("SELECT count(*) FROM {}").format(
                            sql.Identifier("olist", name)
                        )
                    ).fetchone()[0]
                    for name in CORE_TABLES
                )
                self.assertEqual(counts, expected_counts[index])
                orphans = connection.execute("""
                    SELECT order_id FROM (
                        SELECT order_id FROM olist.order_items
                        UNION SELECT order_id FROM olist.order_payments
                    ) children WHERE NOT EXISTS (
                        SELECT 1 FROM olist.orders parent WHERE parent.order_id = children.order_id
                    ) ORDER BY order_id;
                """).fetchall()
                self.assertEqual(
                    orphans, [(self.ids["order_3"],)] if index == 1 else []
                )
                if index == 0:
                    ties = connection.execute(
                        "SELECT count(DISTINCT source_updated_at), count(DISTINCT source_sequence) FROM olist.orders;"
                    ).fetchone()
                    self.assertEqual(ties, (1, 2))
            order = connection.execute(
                "SELECT order_status, source_sequence, source_updated_at FROM olist.orders WHERE order_id=%s",
                (self.ids["order_1"],),
            ).fetchone()
            self.assertEqual(
                order, ("delivered", 5, datetime(2020, 1, 3, 12, tzinfo=timezone.utc))
            )
            customer = connection.execute(
                "SELECT customer_city, source_sequence, source_updated_at FROM olist.customers WHERE customer_id=%s",
                (self.ids["customer_1"],),
            ).fetchone()
            self.assertEqual(
                customer,
                (
                    "fixture backdated correction",
                    5,
                    datetime(2020, 1, 1, tzinfo=timezone.utc),
                ),
            )
            self.assertEqual(
                connection.execute(
                    "SELECT sum(operation_count) FROM audit.source_batch_runs;"
                ).fetchone()[0],
                16,
            )
        before = self.state()
        with self.connection() as connection:
            for path in self.batches:
                self.assertEqual(
                    apply_batch(connection, path, self.contract)["status"], "NOOP"
                )
        self.assertEqual(before, self.state())

    def test_same_id_different_payload_fails_without_mutations(self) -> None:
        self.apply_initial()
        _, payload, expected = deepcopy(fixture_batches()[0])
        payload["operations"][0]["values"]["customer_city"] = "different payload"
        changed = self.directory / "changed"
        write_batch(changed, payload, self.contract, expected)
        before = self.state()
        with (
            self.connection() as connection,
            self.assertRaisesRegex(BatchError, "different payload"),
        ):
            apply_batch(connection, changed, self.contract)
        self.assertEqual(before, self.state())

    def test_missing_predecessor_fails_before_allocating_versions(self) -> None:
        before = self.state()
        with (
            self.connection() as connection,
            self.assertRaisesRegex(BatchError, "Predecessor"),
        ):
            apply_batch(connection, self.batches[1], self.contract)
        self.assertEqual(before, self.state())

    def test_unseeded_snapshot_is_rejected_before_mutation(self) -> None:
        self.psql("DELETE FROM audit.source_seed_runs;")
        before = self.state()
        with (
            self.connection() as connection,
            self.assertRaisesRegex(BatchError, "must be seeded"),
        ):
            apply_batch(connection, self.batches[0], self.contract)
        self.assertEqual(before, self.state())

    def test_concurrent_replay_applies_only_once(self) -> None:
        def worker() -> str:
            with self.connection() as connection:
                connection.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
                return apply_batch(connection, self.batches[0], self.contract)["status"]

        with ThreadPoolExecutor(max_workers=2) as executor:
            statuses = list(executor.map(lambda _: worker(), range(2)))
        self.assertEqual(sorted(statuses), ["APPLIED", "NOOP"])
        state = self.state()
        self.assertEqual([len(state[name]) for name in CORE_TABLES], [2, 2, 2, 1])
        self.assertEqual(len(state["audit"]), 1)
        self.assertEqual(
            state["customers_sequence"], {"last_value": 2, "is_called": True}
        )

    def test_audit_failure_rolls_back_and_same_batch_can_retry(self) -> None:
        self.psql("""
            CREATE FUNCTION public.reject_batch_audit() RETURNS trigger LANGUAGE plpgsql
                AS $$ BEGIN RAISE EXCEPTION 'injected batch audit failure'; END; $$;
            CREATE TRIGGER reject_batch_audit BEFORE INSERT ON audit.source_batch_runs
                FOR EACH STATEMENT EXECUTE FUNCTION public.reject_batch_audit();
        """)
        before = self.state(sequences=False)
        with (
            self.connection() as connection,
            self.assertRaisesRegex(psycopg.Error, "injected batch audit failure"),
        ):
            apply_batch(connection, self.batches[0], self.contract)
        self.assertEqual(before, self.state(sequences=False))
        self.psql(
            "DROP TRIGGER reject_batch_audit ON audit.source_batch_runs; DROP FUNCTION public.reject_batch_audit();"
        )
        self.apply_initial()
        self.assertEqual(len(self.state()["audit"]), 1)

    def test_missing_update_rolls_back_prior_changes_and_audit(self) -> None:
        self.apply_initial()
        operation = {
            "table": "customers",
            "operation": "UPDATE",
            "key": {"customer_id": self.ids["customer_1"]},
            "values": {"customer_city": "must rollback"},
            "source_updated_at": "2020-01-06T00:00:00Z",
        }
        missing = deepcopy(operation)
        missing["key"]["customer_id"] = "missing-customer"
        path = self.custom_batch("missing_update", [operation, missing])
        before = self.state(sequences=False)
        with (
            self.connection() as connection,
            self.assertRaisesRegex(BatchError, "did not affect"),
        ):
            apply_batch(connection, path, self.contract)
        self.assertEqual(before, self.state(sequences=False))

    def test_parameterized_text_and_composite_updates(self) -> None:
        self.apply_initial()
        text = "O'Brien \\ unicode Đà Nẵng'); DROP TABLE olist.orders; --"
        path = self.custom_batch(
            "quoted_update",
            [
                {
                    "table": "customers",
                    "operation": "UPDATE",
                    "key": {"customer_id": self.ids["customer_1"]},
                    "values": {"customer_city": text},
                    "source_updated_at": "2020-01-06T00:00:00Z",
                },
                {
                    "table": "order_items",
                    "operation": "UPDATE",
                    "key": {"order_id": self.ids["order_2"], "order_item_id": 1},
                    "values": {"price": "9.99"},
                    "source_updated_at": "2020-01-06T00:00:00Z",
                },
                {
                    "table": "order_payments",
                    "operation": "UPDATE",
                    "key": {"order_id": self.ids["order_1"], "payment_sequential": 1},
                    "values": {"payment_value": "9.99"},
                    "source_updated_at": "2020-01-06T00:00:00Z",
                },
            ],
        )
        with self.connection() as connection:
            apply_batch(connection, path, self.contract)
            self.assertEqual(
                connection.execute(
                    "SELECT customer_city FROM olist.customers WHERE customer_id=%s",
                    (self.ids["customer_1"],),
                ).fetchone()[0],
                text,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT price::text FROM olist.order_items WHERE order_id=%s",
                    (self.ids["order_2"],),
                ).fetchone()[0],
                "9.99",
            )
            self.assertEqual(
                connection.execute(
                    "SELECT price::text FROM olist.order_items WHERE order_id=%s",
                    (self.ids["order_1"],),
                ).fetchone()[0],
                "10.50",
            )
            self.assertEqual(
                connection.execute(
                    "SELECT payment_value::text FROM olist.order_payments WHERE order_id=%s",
                    (self.ids["order_1"],),
                ).fetchone()[0],
                "9.99",
            )

    def test_loader_and_reader_audit_permissions(self) -> None:
        self.apply_initial()
        with self.connection("reader") as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM audit.source_batch_runs"
                ).fetchone()[0],
                1,
            )
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                connection.execute(
                    "UPDATE olist.customers SET customer_city='forbidden'"
                )
        with self.connection() as connection:
            for query in [
                "DELETE FROM audit.source_batch_runs",
                "UPDATE audit.source_batch_runs SET status='SUCCESS'",
            ]:
                with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    connection.execute(query)

    def test_cli_connects_using_dsn_and_standard_pg_environment(self) -> None:
        contract_path = self.directory / "contract.json"
        contract_path.write_text(json.dumps(self.contract), encoding="utf-8")
        configurations = [
            {
                "SOURCE_SIMULATOR_DSN": make_conninfo(
                    host="127.0.0.1",
                    port=self.port,
                    dbname=self.database,
                    user="olist_test_loader",
                    password="isolated_test_loader",
                )
            },
            {
                "PGHOST": "127.0.0.1",
                "PGPORT": str(self.port),
                "PGDATABASE": self.database,
                "PGUSER": "olist_test_loader",
                "PGPASSWORD": "isolated_test_loader",
            },
        ]
        for configuration, expected in zip(configurations, ["APPLIED", "NOOP"]):
            environment = os.environ.copy()
            environment.pop("SOURCE_SIMULATOR_DSN", None)
            environment.pop("PYTHONPATH", None)
            environment.update(configuration)
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "olist_platform.source_simulator.cli",
                    "--contract",
                    str(contract_path),
                    "apply",
                    "--batch-dir",
                    str(self.batches[0]),
                ],
                cwd=self.directory,
                env=environment,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)["status"], expected)


if __name__ == "__main__":
    unittest.main()
