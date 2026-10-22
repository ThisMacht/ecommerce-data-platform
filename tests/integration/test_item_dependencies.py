from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

from bronze_case import BronzeFixtureCase

from olist_platform.bronze.extraction import ingest_source
from olist_platform.bronze.storage import BronzeError, encode
from olist_platform.deferred.lifecycle import register_silver, resolve_snapshot
from olist_platform.silver.batch import ITEM_SCOPE, process_batch, verify_silver
from olist_platform.silver.orders import spark_session
from olist_platform.warehouse.loading import _merge_rows, load_silver, read_route


class ItemDependenciesTest(BronzeFixtureCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)

    def setUp(self):
        super().setUp()
        self.psql("""
            INSERT INTO olist.products (product_id,product_weight_g,source_updated_at,source_operation,source_batch_id)
            VALUES ('product',10,'2020-01-01','INSERT','fixture'),
                   ('rejected_product',-1,'2020-01-01','INSERT','fixture');
            INSERT INTO olist.sellers (seller_id,seller_zip_code_prefix,seller_city,seller_state,source_updated_at,source_operation,source_batch_id)
            VALUES ('seller','00123','São Paulo','SP','2020-01-01','INSERT','fixture');
            INSERT INTO olist.order_items
                (order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value,source_updated_at,source_operation,source_batch_id)
            VALUES ('order_a',1,'product','seller','2018-01-01',10.50,1.25,'2020-01-04','INSERT','fixture'),
                   ('order_a',2,'late_product','seller','2018-01-01',10.50,1.25,'2020-01-04','INSERT','fixture'),
                   ('order_a',3,'product','late_seller','2018-01-01',10.50,1.25,'2020-01-04','INSERT','fixture'),
                   ('late_order',4,'late_product','late_seller','2018-01-01',10.50,1.25,'2020-01-04','INSERT','fixture'),
                   ('unknown_order',5,'product','seller','2018-01-01',10.50,1.25,'2020-01-04','INSERT','fixture'),
                   ('invalid_order',6,'absent','absent','2018-01-01',-1,1.25,'2020-01-04','INSERT','fixture'),
                   ('order_a',7,'','seller','2018-01-01',10.50,1.25,'2020-01-04','INSERT','fixture'),
                   ('order_a',8,'rejected_product','seller','2018-01-01',10.50,1.25,'2020-01-04','INSERT','fixture');
        """)

    def bronze(self, table, identity="first", **kwargs):
        with self.connection() as c:
            return ingest_source(
                c,
                self.store,
                self.contract,
                table=table,
                run_id=identity,
                endpoint=self.endpoint,
                full_snapshot=table == "orders",
                **kwargs,
            )["manifest"]

    def parent(self, table, identity="parents", **kwargs):
        return process_batch(
            self.spark,
            self.store,
            self.contract,
            table=table,
            bronze_prefix=self.bronze(table, identity)["prefix"],
            run_id=identity,
            **kwargs,
        )["manifest"]

    def parents(self):
        return {
            table: self.parent(table) for table in ("orders", "products", "sellers")
        }

    def process(self, bronze, parents, identity="items", store=None):
        return process_batch(
            self.spark,
            store or self.store,
            self.contract,
            table="order_items",
            bronze_prefix=bronze["prefix"],
            run_id=identity,
            order_snapshot=parents["orders"]["prefix"],
            product_snapshot=parents["products"]["prefix"],
            seller_snapshot=parents["sellers"]["prefix"],
        )

    def rows(self, manifest, route="valid"):
        with tempfile.TemporaryDirectory() as directory:
            return list(
                read_route(self.spark, self.store, manifest, Path(directory), route)
            )

    def register(self, manifest, identity="register", attempts=5):
        with self.connection() as c:
            return register_silver(
                self.spark,
                c,
                self.store,
                self.contract,
                table="order_items",
                run_id=identity,
                silver_prefix=manifest["prefix"],
                max_attempts=attempts,
            )

    def resolve(self, parents, identity="retry", strict=True):
        with self.connection() as c:
            return resolve_snapshot(
                self.spark,
                c,
                self.store,
                self.contract,
                table="order_items",
                run_id=identity,
                parent_snapshot=parents["orders"]["prefix"],
                **(
                    {
                        "product_snapshot": parents["products"]["prefix"],
                        "seller_snapshot": parents["sellers"]["prefix"],
                    }
                    if strict
                    else {}
                ),
            )

    def state(self):
        return self.psql(
            "SELECT COALESCE(jsonb_agg(to_jsonb(q) ORDER BY record_uid),'[]') FROM audit.quarantine_records q"
        )

    def test_three_pinned_fks_partial_arrivals_atomic_retry_and_original_lineage(self):
        parents = self.parents()
        bronze = self.bronze("order_items")
        original = self.process(bronze, parents)["manifest"]
        self.assertEqual(original["schema_version"], "order_items-silver-v2")
        self.assertEqual(original["relationship_checks"], ITEM_SCOPE)
        self.assertEqual(original["unchecked_relationships"], [])
        self.assertEqual(
            original["counts"],
            dict(
                input_rows=8,
                valid_rows=1,
                deferred_rows=5,
                rejected_rows=2,
                duplicates_rows=0,
                warning_rows=0,
            ),
        )
        pending = self.rows(original, "deferred")
        rules = {r["order_item_id"]: r["deferred_rule_ids"] for r in pending}
        self.assertEqual(
            rules,
            {
                2: ["missing_product"],
                3: ["missing_seller"],
                4: ["missing_order", "missing_product", "missing_seller"],
                5: ["missing_order"],
                8: ["missing_product"],
            },
        )
        self.assertTrue(
            all(r["deferred_rule_ids"] == [] for r in self.rows(original, "rejected"))
        )
        self.assertEqual(self.process(bronze, parents)["status"], "NOOP")
        with self.connection() as c:
            load_silver(
                self.spark,
                c,
                self.store,
                self.contract,
                table="order_items",
                run_id="items",
                silver_prefix=original["prefix"],
            )
        self.assertEqual(self.register(original)["inserted_rows"], 5)
        self.assertEqual(self.register(original)["status"], "NOOP")
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) || '|' || SUM(jsonb_array_length(payload->'deferred_rule_ids')) FROM audit.quarantine_records"
            ),
            "5|7",
        )
        before = self.state()
        self.assertEqual(
            self.resolve(parents, "legacy", strict=False)["deferred_retry_input_rows"],
            0,
        )
        self.assertEqual(self.state(), before)
        self.assertEqual(self.resolve(parents)["routing_counts"]["deferred_rows"], 5)
        self.assertEqual(self.resolve(parents)["status"], "NOOP")
        # Freeze original items; retries never re-read source or rewrite Silver.
        original_body = self.store.read(f"{original['prefix']}/manifest.json")
        self.psql("""DELETE FROM olist.order_items;
            INSERT INTO olist.products (product_id,source_updated_at,source_operation,source_batch_id)
            VALUES ('late_product','2019-01-01','INSERT','late');
            UPDATE olist.products SET product_weight_g=10,source_sequence=DEFAULT WHERE product_id='rejected_product';""")
        partial = parents | {"products": self.parent("products", "products_late")}
        with self.assertRaises(BronzeError):
            self.process(bronze, partial)
        with self.assertRaisesRegex(BronzeError, "request conflict"):
            self.resolve(partial)
        result = self.resolve(partial, "partial")
        self.assertEqual(
            (
                result["deferred_retry_input_rows"],
                result["routing_counts"]["valid_rows"],
                result["routing_counts"]["deferred_rows"],
            ),
            (5, 2, 3),
        )
        self.assertEqual(
            sorted(result["missing_dependencies"].values()),
            [
                ["missing_order"],
                ["missing_order", "missing_seller"],
                ["missing_seller"],
            ],
        )
        self.psql("""INSERT INTO olist.orders
            (order_id,customer_id,order_status,order_purchase_timestamp,order_estimated_delivery_date,source_updated_at,source_operation,source_batch_id)
            VALUES ('late_order','customer_a','approved','2018-01-01','2018-01-10','2019-01-01','INSERT','late'),
                   ('unknown_order','customer_a','approved','2018-01-01','2018-01-10','2019-01-01','INSERT','late');
            INSERT INTO olist.sellers (seller_id,seller_city,seller_state,seller_zip_code_prefix,source_updated_at,source_operation,source_batch_id)
            VALUES ('late_seller','Rio','RJ','00123','2019-01-01','INSERT','late');""")
        complete = partial | {
            "orders": self.parent("orders", "orders_late"),
            "sellers": self.parent("sellers", "sellers_late"),
        }
        for name in ("orders", "sellers"):
            with (
                self.subTest(parent=name),
                self.assertRaisesRegex(BronzeError, "request conflict"),
            ):
                self.resolve(partial | {name: complete[name]}, "partial")
        before = self.state()

        def fail_after_merge(*args, **kwargs):
            _merge_rows(*args, **kwargs)
            raise RuntimeError("failure after real merge")

        with patch(
            "olist_platform.deferred.lifecycle._merge_rows",
            side_effect=fail_after_merge,
        ):
            with self.assertRaises(RuntimeError):
                self.resolve(complete, "complete")
        self.assertEqual(self.state(), before)
        self.assertEqual(self.psql("SELECT COUNT(*) FROM raw_staging.order_items"), "3")
        result = self.resolve(complete, "complete")
        self.assertEqual(result["routing_counts"]["valid_rows"], 3)
        self.assertEqual(self.resolve(complete, "complete")["status"], "NOOP")
        self.assertEqual(self.psql("SELECT COUNT(*) FROM raw_staging.order_items"), "6")
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM audit.quarantine_records WHERE retry_status='RESOLVED'"
            ),
            "5",
        )
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM raw_staging.order_items WHERE price=10.50 AND freight_value=1.25"
            ),
            "6",
        )
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM raw_staging.order_items t JOIN audit.quarantine_records q ON q.record_uid=t._record_uid AND q.run_id=t._run_id"
            ),
            "5",
        )
        self.assertEqual(
            self.store.read(f"{original['prefix']}/manifest.json"), original_body
        )
        self.assertEqual(verify_silver(self.store, original["prefix"]), original)

    def test_expiry_counts_physical_rows_and_never_reopens(self):
        parents = self.parents()
        original = self.process(self.bronze("order_items"), parents)["manifest"]
        self.register(original, attempts=1)
        result = self.resolve(parents)
        self.assertEqual(result["routing_counts"]["rejected_rows"], 5)
        self.assertEqual(result["deferred_retry_input_rows"], 5)
        self.assertEqual(self.psql("SELECT COUNT(*) FROM audit.deferred_attempts"), "5")
        self.assertEqual(self.psql("SELECT COUNT(*) FROM raw_staging.order_items"), "0")
        self.assertEqual(
            self.register(original, "again", attempts=1)["inserted_rows"], 0
        )
        self.assertEqual(
            self.resolve(parents, "terminal")["deferred_retry_input_rows"], 0
        )
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM audit.quarantine_records WHERE retry_status='EXPIRED'"
            ),
            "5",
        )

    def test_invalid_pin_scope_missing_flags_corruption_and_sealed_retry(self):
        parents = self.parents()
        bronze = self.bronze("order_items")
        for options in (
            {"product_snapshot": parents["products"]["prefix"]},
            {"seller_snapshot": parents["sellers"]["prefix"]},
        ):
            with (
                self.subTest(options=options),
                self.assertRaisesRegex(BronzeError, "configuration"),
            ):
                process_batch(
                    self.spark,
                    self.store,
                    self.contract,
                    table="order_items",
                    run_id="invalid",
                    bronze_prefix=bronze["prefix"],
                    order_snapshot=parents["orders"]["prefix"],
                    **options,
                )
        with self.assertRaisesRegex(BronzeError, "full verified snapshot"):
            self.process(bronze, parents | {"products": parents["orders"]})
        for name in ("products", "sellers"):
            obj = parents[name]["objects"][0]
            body = self.store.read(obj["key"])
            self.client.put_object(
                Bucket=self.store.bucket, Key=obj["key"], Body=b"corrupt"
            )
            with (
                self.subTest(parent=name),
                self.assertRaisesRegex(BronzeError, "checksum"),
            ):
                self.process(bronze, parents)
            self.client.put_object(Bucket=self.store.bucket, Key=obj["key"], Body=body)
        with self.assertRaises(RuntimeError):
            self.process(bronze, parents, store=self.failure_store("_SUCCESS"))
        result = self.process(bronze, parents)
        self.assertEqual(result["status"], "PUBLISHED")
        self.assertEqual(self.process(bronze, parents)["status"], "NOOP")
        self.register(result["manifest"])
        for name in ("products", "sellers"):
            obj = parents[name]["objects"][0]
            body = self.store.read(obj["key"])
            self.client.put_object(
                Bucket=self.store.bucket, Key=obj["key"], Body=b"corrupt"
            )
            with (
                self.subTest(parent=name),
                self.assertRaisesRegex(BronzeError, "checksum"),
            ):
                self.resolve(parents)
            self.client.put_object(Bucket=self.store.bucket, Key=obj["key"], Body=body)
        self.assertEqual(
            self.psql("SELECT SUM(attempt_count) FROM audit.quarantine_records"), "0"
        )

    def test_empty_full_parents_defer_all_locally_valid_items(self):
        self.psql(
            "DELETE FROM olist.orders; DELETE FROM olist.products; DELETE FROM olist.sellers"
        )
        original = self.process(self.bronze("order_items"), self.parents())["manifest"]
        self.assertEqual(original["counts"]["deferred_rows"], 6)
        self.assertEqual(original["counts"]["rejected_rows"], 2)
        self.assertTrue(
            all(
                r["deferred_rule_ids"]
                == ["missing_order", "missing_product", "missing_seller"]
                for r in self.rows(original, "deferred")
            )
        )

    def test_cli_pins_three_parents_and_uses_strict_retry_scope(self):
        parents = self.parents()
        bronze = self.bronze("order_items")
        env = os.environ.copy()
        env.pop("WAREHOUSE_DSN", None)
        env.update(
            PGHOST="127.0.0.1",
            PGPORT=str(self.port),
            PGDATABASE=self.database,
            PGUSER="olist_test_loader",
            PGPASSWORD="isolated_test_loader",
            AWS_ACCESS_KEY_ID="bronze_test_admin",
            AWS_SECRET_ACCESS_KEY="isolated_bronze_password",
        )
        with tempfile.TemporaryDirectory() as directory:
            contract = Path(directory) / "contract.json"
            contract.write_bytes(encode(self.contract))
            common = [
                "--contract",
                str(contract),
                "--endpoint",
                self.endpoint,
                "--bucket",
                self.store.bucket,
                "--table",
                "order_items",
            ]
            pins = [
                "--product-snapshot",
                parents["products"]["prefix"],
                "--seller-snapshot",
                parents["sellers"]["prefix"],
            ]

            def cli(module, options):
                result = subprocess.run(
                    [sys.executable, "-m", f"olist_platform.{module}.cli"]
                    + common
                    + options,
                    env=env,
                    cwd=directory,
                    capture_output=True,
                    text=True,
                    timeout=120,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                return json.loads(result.stdout)

            manifest = cli(
                "silver",
                [
                    "process",
                    "--run-id",
                    "cli",
                    "--bronze-prefix",
                    bronze["prefix"],
                    "--order-snapshot",
                    parents["orders"]["prefix"],
                ]
                + pins,
            )["manifest"]
            self.assertEqual(manifest["schema_version"], "order_items-silver-v2")
            self.assertEqual(
                cli(
                    "deferred",
                    [
                        "register",
                        "--run-id",
                        "cli",
                        "--silver-prefix",
                        manifest["prefix"],
                    ],
                )["inserted_rows"],
                5,
            )
            result = cli(
                "deferred",
                [
                    "resolve",
                    "--run-id",
                    "cli",
                    "--parent-snapshot",
                    parents["orders"]["prefix"],
                ]
                + pins,
            )
            self.assertEqual(result["deferred_retry_input_rows"], 5)
            self.assertEqual(result["routing_counts"]["deferred_rows"], 5)
