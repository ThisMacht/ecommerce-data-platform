from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import psycopg
from postgres_case import PostgresIntegrationCase

from olist_platform.bronze.storage import BronzeError, encode, sha256
from olist_platform.deferred.lifecycle import register_rows, resolve_rows
from olist_platform.warehouse.loading import merge_rows
from tests.warehouse_fixture import valid_row


class DeferredStateTest(PostgresIntegrationCase):
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

    def row(self, table="orders", key="a", sequence=1):
        row = valid_row(table, key, sequence)
        row["_record_uid"] = sha256(encode([table, key, sequence]))
        row["deferred_rule_ids"] = [
            "missing_customer" if table == "orders" else "missing_order"
        ]
        row["_raw_payload"] = encode(
            {"independent": "original_source", "key": key}
        ).decode()
        return {
            name: value.isoformat() if isinstance(value, datetime) else value
            for name, value in row.items()
        }

    def register(
        self,
        rows=None,
        table="orders",
        run_id="register",
        attempts=3,
        hours=168,
        expected=None,
    ):
        rows = rows if rows is not None else [self.row(table)]
        request = {
            "max_attempts": attempts,
            "max_deferred_age_hours": hours,
            "contract_sha256": sha256(encode(self.contract)),
            "silver_prefix": f"silver/{table}/run_id=original",
            "silver_manifest_sha256": "a" * 64,
        }
        with self.connection() as connection:
            return register_rows(
                connection,
                self.contract,
                table=table,
                run_id=run_id,
                rows=rows,
                request=request,
                expected_count=len(rows) if expected is None else expected,
            )

    def resolve(self, keys=None, table="orders", run_id="retry", parent="parent-v1"):
        request = {"parent": parent, "contract_sha256": sha256(encode(self.contract))}
        with self.connection() as connection:
            return resolve_rows(
                connection,
                self.contract,
                table=table,
                run_id=run_id,
                parent_keys=set(keys or []),
                request=request,
            )

    def state(self):
        return json.loads(
            self.psql(
                "SELECT COALESCE(jsonb_agg(to_jsonb(q) ORDER BY record_uid),'[]') FROM audit.quarantine_records q"
            )
        )

    def test_replay_late_parent_lineage_and_atomic_load_for_all_dependent_tables(self):
        for table in ("orders", "order_items", "order_payments"):
            with self.subTest(table=table):
                row = self.row(table)
                self.assertEqual(self.register([row], table)["inserted_rows"], 1)
                self.assertEqual(self.register([row], table)["status"], "NOOP")
                pending = self.resolve(table=table)
                self.assertEqual(pending["routing_counts"]["deferred_rows"], 1)
                self.assertEqual(pending["deferred_retry_input_rows"], 1)
                self.assertEqual(pending["source_rows_extracted"], 0)
                before = self.state()
                self.assertEqual(self.resolve(table=table)["status"], "NOOP")
                self.assertEqual(self.state(), before)
                parent = "parent" if table == "orders" else "a"
                resolved = self.resolve([parent], table, "retry_late", "parent-v2")
                self.assertEqual(resolved["routing_counts"]["valid_rows"], 1)
                self.assertEqual(
                    resolved["warehouse_counts"]["warehouse_inserted_rows"], 1
                )
                target = json.loads(
                    self.psql(f"SELECT row_to_json(t) FROM raw_staging.{table} t")
                )
                self.assertEqual(target["_record_uid"], row["_record_uid"])
                self.assertEqual(target["_run_id"], row["_run_id"])
                self.assertEqual(
                    self.register([row], table, "seen_again")["inserted_rows"], 0
                )
                self.assertEqual(
                    self.resolve([parent], table, "terminal")[
                        "deferred_retry_input_rows"
                    ],
                    0,
                )
                with self.assertRaisesRegex(BronzeError, "conflict"):
                    self.resolve([parent], table, "retry_late", "different-parent")

    def test_attempt_and_age_expiry_are_terminal_rejects_and_final_attempt_can_resolve(
        self,
    ):
        rows = [
            self.row(key=key, sequence=number)
            for number, key in enumerate(("age", "attempts", "last_chance"), 1)
        ]
        self.register([rows[0]], run_id="age", hours=1)
        self.register([rows[1]], run_id="attempts", attempts=2)
        self.register([rows[2]], run_id="chance", attempts=2)
        self.psql(
            "UPDATE audit.quarantine_records SET first_seen_at=clock_timestamp()-INTERVAL '2 HOURS' WHERE source_primary_key->>'order_id'='age'"
        )
        first = self.resolve()
        self.assertEqual(
            (
                first["routing_counts"]["rejected_rows"],
                first["routing_counts"]["deferred_rows"],
            ),
            (1, 2),
        )
        # All rows share the same parent key; last chance is tested in its own dataset.
        second = self.resolve(run_id="second")
        self.assertEqual(second["routing_counts"]["rejected_rows"], 2)
        self.assertEqual(
            {r["terminal_reason"] for r in self.state()},
            {"max_deferred_age", "max_attempts"},
        )
        self.assertEqual(
            self.resolve(["parent"], run_id="third")["deferred_retry_input_rows"], 0
        )
        self.assertEqual(self.psql("SELECT COUNT(*) FROM raw_staging.orders"), "0")
        self.register([self.row("order_items")], table="order_items", attempts=2)
        self.resolve(table="order_items")
        result = self.resolve(["a"], table="order_items", run_id="last_chance")
        self.assertEqual(result["routing_counts"]["valid_rows"], 1)

    def test_payload_policy_duplicates_and_counts_fail_registration_atomically(self):
        row = self.row()
        with self.assertRaisesRegex(BronzeError, "row count"):
            self.register([row], expected=2)
        with self.assertRaisesRegex(BronzeError, "Repeated"):
            self.register([row, row])
        self.assertEqual(self.state(), [])
        self.register([row])
        before = self.state()
        with self.assertRaisesRegex(BronzeError, "request conflict"):
            self.register([row], attempts=4)
        with self.assertRaisesRegex(BronzeError, "payload/policy conflict"):
            self.register([row | {"order_status": "shipped"}], run_id="changed")
        self.assertEqual(self.state(), before)

    def test_merge_conflict_or_resolution_audit_failure_rolls_back_queue_and_data(self):
        self.register()
        with self.connection() as connection:
            merge_rows(
                connection,
                self.contract,
                table="orders",
                run_id="baseline",
                rows=[valid_row("orders") | {"order_status": "shipped"}],
                request={"fixture": "existing"},
                silver_counts={
                    "input_rows": 1,
                    "valid_rows": 1,
                    "deferred_rows": 0,
                    "rejected_rows": 0,
                    "duplicates_rows": 0,
                    "warning_rows": 0,
                },
            )
        before = self.state()
        with self.assertRaisesRegex(BronzeError, "Conflicting"):
            self.resolve(["parent"])
        self.assertEqual(self.state(), before)
        self.psql(
            "TRUNCATE raw_staging.orders; CREATE FUNCTION audit.fail_resolution() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected resolution failure'; END $$; CREATE TRIGGER fail_resolution BEFORE INSERT ON audit.deferred_resolution_runs FOR EACH ROW EXECUTE FUNCTION audit.fail_resolution();"
        )
        with self.assertRaises(psycopg.Error):
            self.resolve(["parent"])
        self.assertEqual(self.state(), before)
        self.assertEqual(self.psql("SELECT COUNT(*) FROM raw_staging.orders"), "0")
        self.assertEqual(self.psql("SELECT COUNT(*) FROM audit.deferred_attempts"), "0")
        self.psql("DROP TRIGGER fail_resolution ON audit.deferred_resolution_runs")
        self.assertEqual(self.resolve(["parent"])["routing_counts"]["valid_rows"], 1)

    def test_concurrent_same_retry_increments_attempts_once_and_stale_load_is_safe(
        self,
    ):
        self.register()
        with ThreadPoolExecutor(max_workers=2) as workers:
            results = list(workers.map(lambda _: self.resolve(), range(2)))
        self.assertEqual(sorted(r["status"] for r in results), ["COMMITTED", "NOOP"])
        self.assertEqual(self.state()[0]["attempt_count"], 1)
        with self.connection() as connection:
            merge_rows(
                connection,
                self.contract,
                table="orders",
                run_id="newer",
                rows=[valid_row("orders", sequence=20)],
                request={"newer": True},
                silver_counts={
                    "input_rows": 1,
                    "valid_rows": 1,
                    "deferred_rows": 0,
                    "rejected_rows": 0,
                    "duplicates_rows": 0,
                    "warning_rows": 0,
                },
            )
        result = self.resolve(["parent"], run_id="late")
        self.assertEqual(result["warehouse_counts"]["warehouse_stale_ignored_rows"], 1)
        self.assertEqual(
            self.psql("SELECT source_sequence FROM raw_staging.orders"), "20"
        )

    def test_tampered_payload_or_contract_is_not_resolved(self):
        self.register()
        self.psql(
            "UPDATE audit.quarantine_records SET payload=jsonb_set(payload,'{order_status}','\"shipped\"')"
        )
        with self.assertRaisesRegex(BronzeError, "checksum"):
            self.resolve(["parent"])
        self.assertEqual(self.state()[0]["attempt_count"], 0)
        self.psql(
            "UPDATE audit.quarantine_records SET payload=jsonb_set(payload,'{order_status}','\"approved\"'), contract_sha256=repeat('0',64)"
        )
        with self.assertRaisesRegex(BronzeError, "checksum"):
            self.resolve(["parent"])
        self.assertEqual(self.state()[0]["attempt_count"], 0)

    def test_repeated_source_versions_from_different_bronze_runs_dedupe_before_merge(
        self,
    ):
        first = self.row()
        replay = first | {"_record_uid": "b" * 64, "_run_id": "another_bronze_run"}
        latest = self.row(sequence=2) | {"order_status": "shipped"}
        self.register([first, replay, latest])
        result = self.resolve(["parent"])
        self.assertEqual(
            (
                result["routing_counts"]["valid_rows"],
                result["routing_counts"]["duplicates_rows"],
            ),
            (1, 2),
        )
        self.assertEqual(len(result["duplicate_record_uids"]), 2)
        self.assertEqual(
            self.psql("SELECT source_sequence FROM raw_staging.orders"), "2"
        )
        self.assertEqual({row["retry_status"] for row in self.state()}, {"RESOLVED"})

    def test_conflicting_same_version_between_pending_occurrences_rolls_back(self):
        first = self.row()
        conflict = first | {"_record_uid": "b" * 64, "order_status": "shipped"}
        self.register([first, conflict])
        before = self.state()
        with self.assertRaisesRegex(BronzeError, "Conflicting deferred"):
            self.resolve(["parent"])
        self.assertEqual(self.state(), before)
        self.assertEqual(self.psql("SELECT COUNT(*) FROM raw_staging.orders"), "0")
