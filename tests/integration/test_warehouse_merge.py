from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import psycopg
from postgres_case import PostgresIntegrationCase

from olist_platform.bronze.storage import BronzeError
from olist_platform.warehouse.loading import merge_rows
from tests.warehouse_fixture import valid_row


class WarehouseMergeTest(PostgresIntegrationCase):
    requires_raw_snapshot = False
    publish_port = True

    def connection(self):
        return psycopg.connect(
            host="127.0.0.1",
            port=self.port,
            dbname=self.database,
            user="olist_test_loader",
            password="isolated_test_loader",
            autocommit=True,
        )

    def load(self, table, rows, run_id="first", expected=None, request=None):
        counts = {
            "input_rows": len(rows),
            "valid_rows": len(rows),
            "rejected_rows": 0,
            "deferred_rows": 0,
            "duplicates_rows": 0,
            "warning_rows": sum(bool(r["warning_rule_ids"]) for r in rows),
        }
        if expected is not None:
            counts["valid_rows"] = expected
        with self.connection() as connection:
            return merge_rows(
                connection,
                self.contract,
                table=table,
                run_id=run_id,
                rows=iter(rows),
                request=request or {"fixture": "merge-v1", "table": table},
                silver_counts=counts,
            )

    def dump(self, table):
        return self.psql(
            f"SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY 1::text), '[]') FROM raw_staging.{table} t"
        )

    def test_all_grains_version_outcomes_hashes_and_replay(self):
        for table, field, changed in (
            ("customers", "customer_city", "rio"),
            ("orders", "order_status", "delivered"),
            ("order_items", "seller_id", "new_seller"),
            ("order_payments", "payment_type", "voucher"),
        ):
            with self.subTest(table=table):
                initial = [
                    valid_row(table, key, sequence)
                    for sequence, key in enumerate(("a", "b", "c", "e"), 1)
                ]
                first = self.load(table, initial)
                self.assertEqual(
                    first["counts"],
                    {
                        "load_candidate_rows": 4,
                        "warehouse_inserted_rows": 4,
                        "warehouse_updated_rows": 0,
                        "warehouse_unchanged_rows": 0,
                        "warehouse_stale_ignored_rows": 0,
                    },
                )
                before = self.dump(table)
                self.assertEqual(self.load(table, initial)["status"], "NOOP")
                self.assertEqual(self.dump(table), before)
                incoming = [
                    valid_row(table, "a", 10) | {field: changed},
                    valid_row(table, "b", 20),
                    valid_row(table, "c", 1) | {field: changed},
                    valid_row(table, "e", 4),
                    valid_row(table, "d", 25),
                ]
                # Timestamp recency is deliberately opposite to authoritative source_sequence.
                incoming[0]["source_updated_at"] = "2019-01-01T00:00:00Z"
                second = self.load(table, incoming, "second")
                self.assertEqual(
                    second["counts"],
                    {
                        "load_candidate_rows": 5,
                        "warehouse_inserted_rows": 1,
                        "warehouse_updated_rows": 1,
                        "warehouse_unchanged_rows": 2,
                        "warehouse_stale_ignored_rows": 1,
                    },
                )
                rows = json.loads(self.dump(table))
                key = "customer_id" if table == "customers" else "order_id"
                indexed = {r[key]: r for r in rows}
                self.assertEqual(indexed["a"][field], changed)
                self.assertEqual(indexed["b"]["source_sequence"], 20)
                self.assertEqual(indexed["c"]["source_sequence"], 3)
                self.assertEqual(indexed["c"]["last_seen_run_id"], "first")
                self.assertEqual(indexed["b"]["first_seen_run_id"], "first")
                self.assertEqual(indexed["b"]["last_seen_run_id"], "second")
                self.assertEqual(indexed["d"]["first_seen_run_id"], "second")
                self.assertEqual(len(rows), 5)

    def test_same_version_conflict_rolls_back_entire_batch_and_retry_succeeds(self):
        self.load("customers", [valid_row()])
        before = self.dump("customers")
        with self.assertRaisesRegex(BronzeError, "Conflicting"):
            self.load(
                "customers",
                [
                    valid_row(key="new", sequence=2),
                    valid_row() | {"customer_city": "conflict"},
                ],
                "conflict",
            )
        self.assertEqual(self.dump("customers"), before)
        self.assertEqual(
            self.psql("SELECT COUNT(*) FROM audit.warehouse_load_runs"), "1"
        )
        result = self.load(
            "customers", [valid_row(key="new", sequence=2), valid_row()], "conflict"
        )
        self.assertEqual(result["counts"]["warehouse_inserted_rows"], 1)

    def test_reconciliation_duplicate_keys_and_uids_fail_without_data_or_audit(self):
        for rows, expected in (
            ([valid_row()], 2),
            ([valid_row(), valid_row(sequence=2)], None),
            ([valid_row(), valid_row(key="b")], None),
        ):
            with (
                self.subTest(expected=expected),
                self.assertRaises((BronzeError, psycopg.Error)),
            ):
                self.load("customers", rows, expected=expected)
            self.assertEqual(json.loads(self.dump("customers")), [])
            self.assertEqual(
                self.psql("SELECT COUNT(*) FROM audit.warehouse_load_runs"), "0"
            )

    def test_audit_insert_failure_rolls_back_data_and_retries(self):
        self.psql("""CREATE FUNCTION audit.reject_load() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected audit failure'; END $$;
            CREATE TRIGGER reject_load BEFORE INSERT ON audit.warehouse_load_runs FOR EACH ROW EXECUTE FUNCTION audit.reject_load();""")
        with self.assertRaises(psycopg.Error):
            self.load("customers", [valid_row()])
        self.assertEqual(json.loads(self.dump("customers")), [])
        self.psql("DROP TRIGGER reject_load ON audit.warehouse_load_runs")
        self.assertEqual(self.load("customers", [valid_row()])["status"], "LOADED")

    def test_run_request_conflicts_empty_batches_and_concurrent_replay(self):
        result = self.load("customers", [])
        self.assertEqual(result["counts"]["load_candidate_rows"], 0)
        with self.assertRaisesRegex(BronzeError, "another request"):
            self.load("customers", [], request={"fixture": "changed"})
        with ThreadPoolExecutor(max_workers=2) as workers:
            results = list(
                workers.map(
                    lambda _: self.load("customers", [valid_row()], "concurrent"),
                    range(2),
                )
            )
        self.assertEqual(sorted(r["status"] for r in results), ["LOADED", "NOOP"])
        self.assertEqual(self.psql("SELECT COUNT(*) FROM raw_staging.customers"), "1")
        self.assertEqual(
            self.psql("SELECT COUNT(*) FROM audit.warehouse_load_runs"), "2"
        )

    def test_non_idle_connections_and_reader_writes_are_rejected(self):
        with self.connection() as connection:
            connection.autocommit = False
            with self.assertRaisesRegex(BronzeError, "idle autocommit"):
                merge_rows(
                    connection,
                    self.contract,
                    table="customers",
                    run_id="first",
                    rows=[],
                    request={},
                    silver_counts={},
                )
        with psycopg.connect(
            host="127.0.0.1",
            port=self.port,
            dbname=self.database,
            user="olist_test_reader",
            password="isolated_test_reader",
            autocommit=True,
        ) as reader:
            self.assertEqual(
                reader.execute("SELECT COUNT(*) FROM raw_staging.customers").fetchone()[
                    0
                ],
                0,
            )
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                reader.execute("DELETE FROM raw_staging.customers")

    def test_nullable_warning_locations_and_maximum_decimal_are_preserved(self):
        customer = valid_row() | {
            "customer_city": None,
            "warning_rule_ids": ["missing_customer_city"],
        }
        self.load("customers", [customer])
        stored = json.loads(self.dump("customers"))[0]
        self.assertIsNone(stored["customer_city"])
        self.assertEqual(stored["customer_zip_code_prefix"], "00123")
        self.assertEqual(stored["warning_rule_ids"], ["missing_customer_city"])
        self.assertEqual(stored["_run_id"], "silver_fixture")
        self.load(
            "order_payments",
            [
                valid_row("order_payments")
                | {"payment_value": Decimal("9999999999999999.99")}
            ],
        )
        self.assertEqual(
            self.psql("SELECT payment_value::text FROM raw_staging.order_payments"),
            "9999999999999999.99",
        )
