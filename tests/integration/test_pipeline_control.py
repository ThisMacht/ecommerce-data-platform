import json
from concurrent.futures import ThreadPoolExecutor

import psycopg
from postgres_case import PostgresIntegrationCase

from olist_platform.bronze.storage import BronzeError
from olist_platform.pipeline.control import TASKS, begin, fail, promote, task
from tests.unit.test_pipeline_contract import dataset_evidence


class PipelineControlTest(PostgresIntegrationCase):
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

    def start(self, run_id="first", request=None):
        with self.connection() as c:
            return begin(c, run_id, request or {"fixture": "pipeline-v1"})

    def tasks(self, c, run_id="first", attempt=1):
        for name in TASKS:
            task(c, run_id, attempt, name, "RUNNING")
            task(c, run_id, attempt, name, "SUCCESS")

    def items(self):
        result = {
            table: dataset_evidence()
            for table in ("customers", "orders", "order_items", "order_payments")
        }
        for table, item in result.items():
            ordinal = {
                "order_items": "order_item_id",
                "order_payments": "payment_sequential",
            }.get(table)
            key = {"customer_id" if table == "customers" else "order_id": "a"}
            if ordinal:
                key[ordinal] = 1
            item["bronze"]["run_upper_cursor"] = {
                "updated_at": "2020-01-01T00:00:00Z",
                "source_sequence": 1,
                "primary_key": key,
            }
        return result

    def control(self):
        return self.psql(
            "SELECT jsonb_agg(to_jsonb(c) ORDER BY source_table) FROM audit.pipeline_control c"
        )

    def test_promotes_all_controls_atomically_and_successful_begin_is_noop(self):
        self.start()
        with self.connection() as c:
            self.tasks(c)
            result = promote(c, "first", 1, self.items(), {"node_count": 43})
        self.assertEqual(result["status"], "SUCCESS")
        before = self.control()
        self.assertEqual(self.start()["status"], "NOOP")
        self.assertEqual(self.control(), before)
        self.assertEqual(
            self.psql("SELECT COUNT(*) FROM audit.pipeline_dataset_runs"), "4"
        )

    def test_failed_tasks_and_reconciliation_keep_watermarks_then_resume(self):
        self.start()
        before = self.control()
        with self.connection() as c:
            task(c, "first", 1, "orders.bronze.extract", "RUNNING")
            fail(c, "first", 1, "orders.bronze.extract")
        self.assertEqual(self.control(), before)
        self.assertEqual(self.start()["attempt"], 2)
        with self.connection() as c:
            with self.assertRaisesRegex(BronzeError, "incomplete"):
                promote(c, "first", 2, self.items(), {})
            self.tasks(c, attempt=2)
            bad = self.items()
            bad["orders"]["load"]["warehouse_inserted_rows"] = 10
            with self.assertRaisesRegex(BronzeError, "reconciliation"):
                promote(c, "first", 2, bad, {})
        self.assertEqual(self.control(), before)
        self.assertEqual(
            self.psql("SELECT COUNT(*) FROM audit.pipeline_dataset_runs"), "0"
        )

    def test_cas_conflict_rolls_back_earlier_table_updates(self):
        self.start()
        with self.connection() as c:
            self.tasks(c)
            c.execute(
                "UPDATE audit.pipeline_control SET cursor_version=1 WHERE source_table='order_items'"
            )
            before = self.control()
            with self.assertRaisesRegex(BronzeError, "compare-and-swap"):
                promote(c, "first", 1, self.items(), {})
        self.assertEqual(self.control(), before)
        self.assertEqual(
            self.psql("SELECT COUNT(*) FROM audit.pipeline_dataset_runs"), "0"
        )

    def test_active_run_config_changes_and_stale_attempts_are_rejected(self):
        self.start()
        with self.assertRaisesRegex(BronzeError, "active"):
            self.start("second")
        with self.assertRaisesRegex(BronzeError, "configuration"):
            self.start(request={"changed": True})
        self.assertEqual(self.start()["attempt"], 2)
        with self.connection() as c:
            with self.assertRaisesRegex(BronzeError, "not active"):
                task(c, "first", 1, "orders.bronze.extract", "RUNNING")

    def test_concurrent_begins_allow_only_one_run(self):
        def start(name):
            try:
                return self.start(name)["status"]
            except BronzeError:
                return "REJECTED"

        with ThreadPoolExecutor(max_workers=2) as workers:
            results = list(workers.map(start, ["one", "two"]))
        self.assertEqual(sorted(results), ["REJECTED", "STARTED"])

    def test_empty_and_backdated_upper_cursors_do_not_regress_controls(self):
        self.start()
        with self.connection() as c:
            self.tasks(c)
            promote(c, "first", 1, self.items(), {})
        prior = json.loads(self.control())
        self.start("second")
        items = self.items()
        items["orders"]["bronze"]["run_upper_cursor"] = None
        items["customers"]["bronze"]["run_upper_cursor"]["updated_at"] = (
            "2019-01-01T00:00:00Z"
        )
        with self.connection() as c:
            self.tasks(c, "second")
            promote(c, "second", 1, items, {})
        after = json.loads(self.control())
        self.assertEqual(
            [r["committed_cursor"] for r in prior],
            [r["committed_cursor"] for r in after],
        )
        self.assertEqual({r["cursor_version"] for r in after}, {2})
