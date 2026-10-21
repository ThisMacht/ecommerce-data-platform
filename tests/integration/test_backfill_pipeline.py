from __future__ import annotations

import os
import subprocess
import tempfile
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from pipeline_case import CorePipelineCase
from postgres_case import REPOSITORY_ROOT, identifier, require_success
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from olist_platform.bronze.extraction import KEYS
from olist_platform.bronze.storage import BronzeError, encode, sha256
from olist_platform.deferred.lifecycle import register_rows
from olist_platform.pipeline.cli import configuration, finish
from olist_platform.pipeline.control import backfill_window, begin
from olist_platform.warehouse.loading import merge_rows
from tests.warehouse_fixture import valid_row


class BackfillPipelineTest(CorePipelineCase):
    def controls(self, c):
        return c.execute(
            "SELECT * FROM audit.pipeline_control WHERE pipeline_name!='backfill_core_v1' ORDER BY pipeline_name,source_table"
        ).fetchall()

    def sentinel(self, c):
        row = valid_row("orders", "unrelated", 999)
        row.update(
            _run_id="unrelated_batch",
            deferred_rule_ids=["missing_customer"],
            _raw_payload='{"independent":"sentinel"}',
        )
        row = {
            k: v.isoformat() if isinstance(v, datetime) else v for k, v in row.items()
        }
        register_rows(
            c,
            self.contract,
            table="orders",
            run_id="unrelated_registration",
            rows=[row],
            expected_count=1,
            request={
                "max_attempts": 3,
                "max_deferred_age_hours": 168,
                "contract_sha256": sha256(encode(self.contract)),
                "silver_prefix": "silver/sentinel",
                "silver_manifest_sha256": "a" * 64,
            },
        )

    def queue(self, c, origin="unrelated_batch"):
        return c.execute(
            "SELECT * FROM audit.quarantine_records WHERE run_id=%s ORDER BY record_uid,rule_id",
            (origin,),
        ).fetchall()

    def snapshots(self, c):
        result = {}
        for table in KEYS:
            columns = [
                x["name"]
                for x in next(
                    t["columns"] for t in self.contract["tables"] if t["table"] == table
                )
            ] + ["record_hash"]
            result[table] = c.execute(
                sql.SQL("SELECT {} FROM raw_staging.{} ORDER BY {}").format(
                    sql.SQL(",").join(map(sql.Identifier, columns)),
                    sql.Identifier(table),
                    sql.SQL(",").join(map(sql.Identifier, KEYS[table])),
                )
            ).fetchall()
        for table, key in (
            ("fct_orders", "order_id"),
            ("mart_revenue_daily", "order_purchase_date"),
            ("mart_average_order_value", "order_purchase_date"),
        ):
            result[table] = c.execute(
                sql.SQL("SELECT * FROM analytics.{} ORDER BY {}").format(
                    sql.Identifier(table), sql.Identifier(key)
                )
            ).fetchall()
        return result

    def full_reference(self, source, directory):
        original = self.database
        reference_name = "reference_" + uuid4().hex[:12]
        self.psql(f"CREATE DATABASE {identifier(reference_name)}", database="postgres")
        self.addCleanup(
            lambda: self.psql(
                f"DROP DATABASE {identifier(reference_name)} WITH (FORCE)",
                database="postgres",
            )
        )
        try:
            self.database = reference_name
            require_success(self.job("run_migrations.sh"))
            reference = self.connection()
        finally:
            self.database = original
        with reference:
            for schema, table in [("olist", t) for t in KEYS] + [
                ("audit", "source_seed_runs")
            ]:
                cursor = source.execute(
                    sql.SQL("SELECT * FROM {}.{}").format(
                        sql.Identifier(schema), sql.Identifier(table)
                    )
                )
                columns = [x.name for x in cursor.description]
                rows = [
                    tuple(Jsonb(v) if isinstance(v, dict) else v for v in row)
                    for row in cursor.fetchall()
                ]
                if rows:
                    with reference.cursor() as writer:
                        writer.executemany(
                            sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                                sql.Identifier(schema),
                                sql.Identifier(table),
                                sql.SQL(",").join(map(sql.Identifier, columns)),
                                sql.SQL(",").join(sql.Placeholder() for _ in columns),
                            ),
                            rows,
                        )
            with patch.dict(
                os.environ, {"DATASET_MODE": "full", "PGDATABASE": reference_name}
            ):
                request = configuration(REPOSITORY_ROOT, self.contract)
                begin(reference, "reference", request)
                self.process(reference, "reference", 1, mode="full")
                finish(
                    reference,
                    self.store,
                    self.contract,
                    REPOSITORY_ROOT,
                    "reference",
                    1,
                    request,
                    directory,
                )
            return self.snapshots(reference)

    def test_empty_window_keeps_staging_and_all_cursors_unchanged(self):
        with (
            self.environment(mode="backfill"),
            patch.dict(
                os.environ,
                {
                    "BACKFILL_START": "2021-01-01T00:00:00Z",
                    "BACKFILL_END": "2021-01-02T00:00:00Z",
                },
            ),
            self.connection() as c,
            tempfile.TemporaryDirectory() as directory,
        ):
            request = configuration(REPOSITORY_ROOT, self.contract)
            begin(c, "empty", request)
            before = c.execute(
                "SELECT * FROM audit.pipeline_control ORDER BY pipeline_name,source_table"
            ).fetchall()
            self.sentinel(c)
            sentinel = self.queue(c)
            self.frozen(c, "empty", 1)
            published = self.process(c, "empty", 1, mode="backfill")
            self.assertTrue(
                all(
                    b["row_count"] == 0 and b["run_upper_cursor"] is None
                    for b, _ in published.values()
                )
            )
            result = finish(
                c,
                self.store,
                self.contract,
                REPOSITORY_ROOT,
                "empty",
                1,
                request,
                Path(directory),
            )
            self.assertEqual(
                sum(x["source_backfill_rows"] for x in result["datasets"].values()), 0
            )
            self.assertEqual(
                c.execute(
                    "SELECT * FROM audit.pipeline_control ORDER BY pipeline_name,source_table"
                ).fetchall(),
                before,
            )
            self.assertEqual(self.queue(c), sentinel)
            self.assertEqual(
                c.execute("SELECT COUNT(*) FROM analytics.fct_orders").fetchone()[0], 0
            )

    def test_range_repair_retry_queue_isolation_and_full_reload_equivalence(self):
        with (
            self.environment(),
            self.connection() as c,
            tempfile.TemporaryDirectory() as directory,
        ):
            root = Path(directory)
            incremental = configuration(REPOSITORY_ROOT, self.contract)
            begin(c, "baseline", incremental)
            self.frozen(c, "baseline", 1)
            self.process(c, "baseline", 1)
            finish(
                c,
                self.store,
                self.contract,
                REPOSITORY_ROOT,
                "baseline",
                1,
                incremental,
                root / "baseline",
            )
            scheduled = self.controls(c)
            self.sentinel(c)
            sentinel = self.queue(c)
            self.psql(
                (
                    REPOSITORY_ROOT / "tests/fixtures/bronze/backfill_updates.sql"
                ).read_text()
            )
            with patch.dict(
                os.environ,
                {
                    "DATASET_MODE": "backfill",
                    "BACKFILL_START": "2020-01-06T00:00:00Z",
                    "BACKFILL_END": "2020-01-08T00:00:00Z",
                },
            ):
                request = configuration(REPOSITORY_ROOT, self.contract)
                begin(c, "repair", request)
                self.frozen(c, "repair", 1)
                with self.assertRaisesRegex(BronzeError, "configuration"):
                    begin(
                        c,
                        "repair",
                        request
                        | {
                            "backfill_window": backfill_window(
                                "2020-01-05T00:00:00Z", "2020-01-08T00:00:00Z"
                            )
                        },
                    )
                self.psql("""UPDATE olist.customers SET customer_city='Santos',source_sequence=500,source_updated_at='2020-01-09T00:00:00Z' WHERE customer_id='customer_a';
                    UPDATE olist.orders SET order_status='shipped',source_sequence=500,source_updated_at='2020-01-09T00:00:00Z' WHERE order_id='order_a';""")
                with c.cursor(row_factory=dict_row) as reader:
                    newer = reader.execute(
                        "SELECT * FROM raw_staging.orders WHERE order_id='order_a'"
                    ).fetchone()
                newer.update(
                    error_rule_ids=[],
                    deferred_rule_ids=[],
                    order_purchase_date=newer["order_purchase_date"].isoformat(),
                    order_status="shipped",
                    source_sequence=500,
                    source_updated_at=datetime.fromisoformat(
                        "2020-01-09T00:00:00+00:00"
                    ),
                )
                merge_rows(
                    c,
                    self.contract,
                    table="orders",
                    run_id="newer_writer",
                    rows=[newer],
                    request={"fixture": "newer"},
                    silver_counts={
                        "input_rows": 1,
                        "valid_rows": 1,
                        "rejected_rows": 0,
                        "deferred_rows": 0,
                        "duplicates_rows": 0,
                        "warning_rows": 0,
                    },
                )
                published = self.process(c, "repair", 1, mode="backfill")
                self.assertEqual(published["customers"][0]["row_count"], 2)
                self.assertEqual(published["orders"][0]["row_count"], 3)
                for bronze, _ in published.values():
                    self.assertTrue(
                        all(
                            "2020-01-06" <= r["source_updated_at"] < "2020-01-08"
                            for r in self.rows(bronze)
                        )
                    )
                self.assertEqual(
                    c.execute(
                        "SELECT customer_city FROM raw_staging.customers WHERE customer_id='customer_a'"
                    ).fetchone()[0],
                    "campinas",
                )
                self.assertEqual(
                    c.execute(
                        "SELECT order_status FROM raw_staging.orders WHERE order_id='order_a'"
                    ).fetchone()[0],
                    "shipped",
                )
                self.assertEqual(self.queue(c), sentinel)
                with patch(
                    "olist_platform.pipeline.cli.subprocess.run",
                    side_effect=subprocess.CalledProcessError(1, "dbt"),
                ):
                    with self.assertRaises(subprocess.CalledProcessError):
                        finish(
                            c,
                            self.store,
                            self.contract,
                            REPOSITORY_ROOT,
                            "repair",
                            1,
                            request,
                            root / "failed",
                        )
                self.assertEqual(self.controls(c), scheduled)
                self.assertEqual(begin(c, "repair", request)["attempt"], 2)
                self.frozen(c, "repair", 2)
                self.assertEqual(
                    self.process(c, "repair", 2, mode="backfill"), published
                )
                result = finish(
                    c,
                    self.store,
                    self.contract,
                    REPOSITORY_ROOT,
                    "repair",
                    2,
                    request,
                    root / "repair",
                )
                self.assertEqual(
                    result["datasets"]["orders"]["warehouse_stale_ignored_rows"], 1
                )
                self.assertEqual(
                    sum(x["source_backfill_rows"] for x in result["datasets"].values()),
                    11,
                )
                self.assertEqual(
                    sum(
                        x["deferred_retry_input_rows"]
                        for x in result["datasets"].values()
                    ),
                    3,
                )
                self.assertEqual(self.controls(c), scheduled)
                self.assertEqual(self.queue(c), sentinel)
                self.assertEqual(
                    c.execute(
                        "SELECT COUNT(*) FROM audit.pipeline_dataset_runs WHERE run_id='repair' AND watermark_after='null'::jsonb AND watermark_candidate!='null'::jsonb"
                    ).fetchone()[0],
                    4,
                )
                self.assertEqual(begin(c, "repair", request)["status"], "NOOP")
            pending_repair = self.queue(c, "repair")
            with patch.dict(
                os.environ,
                {
                    "DATASET_MODE": "backfill",
                    "BACKFILL_START": "2019-01-01T00:00:00Z",
                    "BACKFILL_END": "2021-01-01T00:00:00Z",
                },
            ):
                request = configuration(REPOSITORY_ROOT, self.contract)
                begin(c, "broad", request)
                self.frozen(c, "broad", 1)
                self.process(c, "broad", 1, mode="backfill")
                finish(
                    c,
                    self.store,
                    self.contract,
                    REPOSITORY_ROOT,
                    "broad",
                    1,
                    request,
                    root / "broad",
                )
            self.assertEqual(self.controls(c), scheduled)
            self.assertEqual(self.queue(c), sentinel)
            self.assertEqual(self.queue(c, "repair"), pending_repair)
            self.assertEqual(
                c.execute(
                    "SELECT MAX(cursor_version) FROM audit.pipeline_control WHERE pipeline_name='backfill_core_v1'"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                c.execute(
                    "SELECT COUNT(*),SUM(gross_paid_amount) FROM analytics.fct_orders"
                ).fetchone(),
                (5, Decimal("56.25")),
            )
            self.assertEqual(
                self.snapshots(c), self.full_reference(c, root / "reference")
            )
