from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import psycopg
from postgres_case import REPOSITORY_ROOT, PostgresIntegrationCase

from olist_platform.warehouse.loading import merge_rows
from olist_platform.warehouse.metabase import configure_reader
from tests.warehouse_fixture import valid_row


class DbtModelsTest(PostgresIntegrationCase):
    requires_raw_snapshot = False
    publish_port = True

    def test_business_dashboard_weighted_aov_date_filters_gate_and_reader_grants(self):
        self.psql("""UPDATE raw_staging.orders SET order_purchase_date='2020-01-02',
            order_purchase_timestamp='2020-01-02T12:00:00Z' WHERE order_id='b';
            UPDATE raw_staging.order_payments SET payment_value=5.00 WHERE order_id='zero';""")
        self.dbt()
        # Isolate the SQL visibility gate; pipeline suites verify actual receipt commits.
        self.psql("""INSERT INTO audit.pipeline_runs
            (run_id,pipeline_name,run_type,request,request_sha256,control_before,status,completed_at)
            VALUES ('published','local_core_v1','manual','{}',repeat('a',64),'{}','SUCCESS',clock_timestamp());""")
        with psycopg.connect(
            host="127.0.0.1",
            port=self.port,
            dbname=self.database,
            user="olist_test_admin",
            password="isolated_test_admin",
            autocommit=True,
        ) as admin:
            configure_reader(admin, "isolated_bi_password")
            configure_reader(admin, "isolated_bi_password")
            admin.execute("ALTER ROLE olist_metabase_reader CREATEDB")
            with self.assertRaisesRegex(RuntimeError, "restricted reader"):
                configure_reader(admin, "isolated_bi_password")
            admin.execute("ALTER ROLE olist_metabase_reader NOCREATEDB")
        specs = {
            c["key"]: c
            for c in json.loads(
                (REPOSITORY_ROOT / "config/metabase_dashboard.json").read_text()
            )["cards"]
        }
        with psycopg.connect(
            host="127.0.0.1",
            port=self.port,
            dbname=self.database,
            user="olist_metabase_reader",
            password="isolated_bi_password",
            autocommit=True,
        ) as reader:

            def query(key, **filters):
                def optional(match):
                    names = re.findall(r"\{\{(\w+)\}\}", match[1])
                    if any(name not in filters for name in names):
                        return ""
                    return re.sub(r"\{\{(\w+)\}\}", r"%(\1)s", match[1])

                statement = re.sub(r"\[\[(.*?)\]\]", optional, specs[key]["query"])
                return reader.execute(statement, filters).fetchall()

            self.assertEqual(query("gmv"), [(Decimal("37.25"),)])
            self.assertEqual(query("paid"), [(Decimal("48.25"),)])
            self.assertEqual(query("aov"), [(Decimal("16.08"),)])
            self.assertEqual(query("eligible"), [(4,)])
            self.assertEqual(query("completeness"), [(4, 2, 1)])
            self.assertEqual(sum(r[1] for r in query("statuses")), 5)
            self.assertEqual(
                query("aov", start_date="2019-12-31", end_date="2019-12-31"),
                [(Decimal("20.00"),)],
            )
            self.assertEqual(
                query("aov", start_date="2020-01-02", end_date="2020-01-02"),
                [(Decimal("8.25"),)],
            )
            self.assertEqual(query("aov", start_date="2050-01-01"), [(None,)])
            self.assertEqual(query("gmv", start_date="2050-01-01"), [(Decimal(0),)])
            new_cards = (
                "top_products",
                "top_sellers",
                "payment_types",
                "daily_payment_types",
            )
            self.assertEqual(
                query("top_products"),
                [("product", Decimal("37.25"), Decimal("6.75"), 3, 2)],
            )
            self.assertEqual(
                query("top_sellers"),
                [("seller", Decimal("37.25"), Decimal("6.75"), 3, 2)],
            )
            self.assertEqual(
                query("payment_types"), [("credit_card", Decimal("48.25"), 4, 3)]
            )
            self.assertEqual(
                query("daily_payment_types"),
                [
                    (date(2019, 12, 31), Decimal("40.00"), 0, 0, 0, 0),
                    (date(2020, 1, 2), Decimal("8.25"), 0, 0, 0, 0),
                ],
            )
            for key in new_cards:
                self.assertEqual(query(key, start_date="2050-01-01"), [])
            self.assertEqual(
                query("top_products", start_date="2020-01-02", end_date="2020-01-02"),
                [("product", Decimal("7.25"), Decimal("1.75"), 1, 1)],
            )
            self.assertEqual(
                query("payment_types", start_date="2020-01-02", end_date="2020-01-02"),
                [("credit_card", Decimal("8.25"), 1, 1)],
            )
            for key in specs:
                query(key)
            for table in (
                "olist.orders",
                "raw_staging.orders",
                "audit.pipeline_runs",
                "analytics.stg_orders",
                "analytics.fct_delivery",
                "analytics.mart_delivery_daily",
                "analytics.fct_payments",
                "analytics.dim_location",
                "analytics.int_geolocation_by_zip",
                "analytics.stg_geolocation",
                "analytics.dim_date",
                "analytics.dim_product",
                "analytics.dim_seller",
                "analytics.fct_order_items",
                "analytics.stg_reviews",
                "analytics.fct_reviews",
                "analytics.mart_review_daily",
                "analytics.mart_customer_value",
                "analytics.mart_repeat_purchase_daily",
                "analytics.mart_order_status_daily",
            ):
                with (
                    self.subTest(table=table),
                    self.assertRaises(psycopg.errors.InsufficientPrivilege),
                ):
                    reader.execute(f"SELECT * FROM {table} LIMIT 1")
            reader.execute("SET default_transaction_read_only=off")
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                reader.execute("UPDATE raw_staging.orders SET order_status='canceled'")
            self.dbt()
            for key in new_cards:
                self.assertTrue(query(key))
            for model in (
                "mart_product_daily",
                "mart_seller_daily",
                "mart_payment_type_daily",
            ):
                for privilege in ("INSERT", "UPDATE", "DELETE"):
                    self.assertFalse(
                        reader.execute(
                            "SELECT has_table_privilege(current_user,%s,%s)",
                            (f"analytics.{model}", privilege),
                        ).fetchone()[0]
                    )
            self.assertEqual(query("gmv"), [(Decimal("37.25"),)])
            self.psql("""INSERT INTO audit.pipeline_runs
                (run_id,pipeline_name,run_type,request,request_sha256,control_before,status)
                VALUES ('pending','local_core_v1','manual','{}',repeat('b',64),'{}','RUNNING');""")
            for key in specs:
                if key != "publication":
                    self.assertEqual(query(key), [])
            self.assertEqual(query("publication")[0][-1], False)
            self.psql(
                "UPDATE audit.pipeline_runs SET status='FAILED',completed_at=clock_timestamp() WHERE run_id='pending'"
            )
            for key in new_cards:
                self.assertEqual(query(key), [])
            self.assertEqual(query("gmv"), [])
            self.psql(
                "UPDATE audit.pipeline_runs SET status='SUCCESS',completed_at=clock_timestamp() WHERE run_id='pending'"
            )
            self.assertEqual(query("gmv"), [(Decimal("37.25"),)])
            self.psql(
                "UPDATE audit.warehouse_load_runs SET committed_at=clock_timestamp() WHERE source_table='orders'"
            )
            self.assertEqual(query("gmv"), [])
            for key in new_cards:
                self.assertEqual(query(key), [])

    def test_product_type1_keys_nulls_hash_and_reconciliation(self):
        def load(row, run_id):
            with psycopg.connect(
                host="127.0.0.1",
                port=self.port,
                dbname=self.database,
                user="olist_test_loader",
                password="isolated_test_loader",
                autocommit=True,
            ) as c:
                return merge_rows(
                    c,
                    self.contract,
                    table="products",
                    run_id=run_id,
                    rows=[row],
                    request={"fixture": run_id},
                    silver_counts={
                        "input_rows": 1,
                        "valid_rows": 1,
                        "rejected_rows": 0,
                        "deferred_rows": 0,
                        "duplicates_rows": 0,
                        "warning_rows": int(bool(row["warning_rule_ids"])),
                    },
                )

        load(valid_row("products", "product_a"), "first")
        load(
            valid_row("products", "product_b", 2)
            | {
                "product_category_name": None,
                "product_weight_g": None,
                "warning_rule_ids": [
                    "missing_product_category_name",
                    "missing_product_weight_g",
                ],
            },
            "nulls",
        )
        self.dbt()
        self.assertEqual(self.psql("SELECT COUNT(*) FROM analytics.dim_product"), "3")
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM analytics.dim_product d FULL JOIN raw_staging.products s USING (product_id) WHERE d.product_id IS NULL OR s.product_id IS NULL OR d.record_hash IS DISTINCT FROM s.record_hash"
            ),
            "0",
        )
        original = json.loads(
            self.psql(
                "SELECT row_to_json(d) FROM analytics.dim_product d WHERE product_id='product_a'"
            )
        )
        self.assertEqual(original["product_key"], "922d5276f919163ea91ecaa181f026cf")
        self.assertEqual(
            (original["product_name_length"], original["product_description_length"]),
            (20, 100),
        )
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM analytics.dim_product WHERE product_id='product_b' AND product_category_name IS NULL AND product_weight_g IS NULL"
            ),
            "1",
        )
        self.assertEqual(
            load(
                valid_row("products", "product_a", 3)
                | {
                    "product_category_name": "new_category",
                    "source_operation": "UPDATE",
                },
                "updated",
            )["counts"]["warehouse_updated_rows"],
            1,
        )
        self.dbt()
        current = json.loads(
            self.psql(
                "SELECT row_to_json(d) FROM analytics.dim_product d WHERE product_id='product_a'"
            )
        )
        self.assertEqual(current["product_key"], original["product_key"])
        self.assertEqual(current["created_at"], original["created_at"])
        self.assertGreater(current["updated_at"], original["updated_at"])
        self.assertNotEqual(current["record_hash"], original["record_hash"])
        self.assertEqual(current["product_category_name"], "new_category")
        self.assertEqual(
            load(valid_row("products", "product_a"), "stale")["counts"][
                "warehouse_stale_ignored_rows"
            ],
            1,
        )
        self.dbt()
        self.assertEqual(
            json.loads(
                self.psql(
                    "SELECT row_to_json(d) FROM analytics.dim_product d WHERE product_id='product_a'"
                )
            ),
            current,
        )

    def test_seller_type1_keys_nulls_hash_and_reconciliation(self):
        def load(row, run_id):
            with psycopg.connect(
                host="127.0.0.1",
                port=self.port,
                dbname=self.database,
                user="olist_test_loader",
                password="isolated_test_loader",
                autocommit=True,
            ) as c:
                return merge_rows(
                    c,
                    self.contract,
                    table="sellers",
                    run_id=run_id,
                    rows=[row],
                    request={"fixture": run_id},
                    silver_counts={
                        "input_rows": 1,
                        "valid_rows": 1,
                        "rejected_rows": 0,
                        "deferred_rows": 0,
                        "duplicates_rows": 0,
                        "warning_rows": int(bool(row["warning_rule_ids"])),
                    },
                )

        load(valid_row("sellers", "seller_a"), "first")
        load(
            valid_row("sellers", "seller_b", 2)
            | {
                "seller_city": None,
                "seller_state": None,
                "seller_zip_code_prefix": None,
                "warning_rule_ids": [
                    "missing_seller_city",
                    "invalid_seller_state",
                    "invalid_seller_zip_code_prefix",
                ],
            },
            "nulls",
        )
        self.dbt()
        self.assertEqual(self.psql("SELECT COUNT(*) FROM analytics.dim_seller"), "3")
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM analytics.dim_seller d FULL JOIN raw_staging.sellers s USING (seller_id) WHERE d.seller_id IS NULL OR s.seller_id IS NULL OR d.record_hash IS DISTINCT FROM s.record_hash"
            ),
            "0",
        )
        original = json.loads(
            self.psql(
                "SELECT row_to_json(d) FROM analytics.dim_seller d WHERE seller_id='seller_a'"
            )
        )
        self.assertEqual(original["seller_key"], "3b500bdcaa411c00bc35eacb0127e372")
        self.assertEqual(
            (
                original["seller_city"],
                original["seller_state"],
                original["seller_zip_code_prefix"],
            ),
            ("são paulo", "SP", "00123"),
        )
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM analytics.dim_seller WHERE seller_id='seller_b' AND seller_city IS NULL AND seller_state IS NULL AND seller_zip_code_prefix IS NULL"
            ),
            "1",
        )
        self.assertEqual(
            load(
                valid_row("sellers", "seller_a", 3)
                | {
                    "seller_city": "rio",
                    "source_operation": "UPDATE",
                },
                "updated",
            )["counts"]["warehouse_updated_rows"],
            1,
        )
        self.dbt()
        current = json.loads(
            self.psql(
                "SELECT row_to_json(d) FROM analytics.dim_seller d WHERE seller_id='seller_a'"
            )
        )
        self.assertEqual(current["seller_key"], original["seller_key"])
        self.assertEqual(current["created_at"], original["created_at"])
        self.assertGreater(current["updated_at"], original["updated_at"])
        self.assertNotEqual(current["record_hash"], original["record_hash"])
        self.assertEqual(current["seller_city"], "rio")
        self.assertEqual(
            load(valid_row("sellers", "seller_a"), "stale")["counts"][
                "warehouse_stale_ignored_rows"
            ],
            1,
        )
        self.dbt()
        self.assertEqual(
            json.loads(
                self.psql(
                    "SELECT row_to_json(d) FROM analytics.dim_seller d WHERE seller_id='seller_a'"
                )
            ),
            current,
        )

    def setUp(self):
        super().setUp()
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        customers = [
            valid_row("customers", "ca", 1),
            valid_row("customers", "cb", 2) | {"customer_city": "rio"},
        ]
        orders = [
            valid_row("orders", key, number)
            | {
                "customer_id": "ca" if key == "a" else "cb",
                "order_status": "canceled" if key == "cancel" else "approved",
            }
            for number, key in enumerate(("a", "b", "cancel", "pending", "zero"), 1)
        ]
        items = [
            valid_row("order_items", key, seq)
            | {
                "order_item_id": ordinal,
                "price": Decimal(price),
                "freight_value": Decimal(freight),
            }
            for seq, (key, ordinal, price, freight) in enumerate(
                (
                    ("a", 1, "10.00", "2.00"),
                    ("a", 2, "20.00", "3.00"),
                    ("b", 1, "7.25", "1.75"),
                    ("cancel", 1, "100.00", "10.00"),
                ),
                1,
            )
        ]
        payments = [
            valid_row("order_payments", key, seq)
            | {"payment_sequential": ordinal, "payment_value": Decimal(amount)}
            for seq, (key, ordinal, amount) in enumerate(
                (
                    ("a", 1, "15.00"),
                    ("a", 2, "20.00"),
                    ("b", 1, "8.25"),
                    ("cancel", 1, "200.00"),
                    ("zero", 1, "0.00"),
                ),
                1,
            )
        ]
        with psycopg.connect(
            host="127.0.0.1",
            port=self.port,
            dbname=self.database,
            user="olist_test_loader",
            password="isolated_test_loader",
            autocommit=True,
        ) as connection:
            for table, rows in (
                ("products", [valid_row("products", "product")]),
                ("sellers", [valid_row("sellers", "seller")]),
                ("customers", customers),
                ("orders", orders),
                ("order_items", items),
                ("order_payments", payments),
            ):
                merge_rows(
                    connection,
                    self.contract,
                    table=table,
                    run_id="fixture",
                    rows=rows,
                    request={"fixture": "dbt-v1"},
                    silver_counts={
                        "input_rows": len(rows),
                        "valid_rows": len(rows),
                        "rejected_rows": 0,
                        "deferred_rows": 0,
                        "duplicates_rows": 0,
                        "warning_rows": 0,
                    },
                )

    def dbt(self, command="build", success=True):
        env = os.environ.copy()
        env.update(
            PGHOST="127.0.0.1",
            PGPORT=str(self.port),
            PGDATABASE=self.database,
            PGUSER="olist_test_loader",
            DBT_ENV_SECRET_PASSWORD="isolated_test_loader",
            DBT_READER_USER="olist_test_reader",
            DBT_SEND_ANONYMOUS_USAGE_STATS="false",
        )
        result = subprocess.run(
            [
                str(Path(sys.executable).parent / "dbt"),
                *command.split(),
                "--project-dir",
                str(REPOSITORY_ROOT / "dbt"),
                "--profiles-dir",
                str(REPOSITORY_ROOT / "dbt"),
                "--target-path",
                self.directory.name,
                "--log-path",
                str(Path(self.directory.name) / "logs"),
            ],
            env=env,
            cwd=self.directory.name,
            capture_output=True,
            text=True,
            timeout=180,
        )
        if success:
            self.assertEqual(result.returncode, 0, result.stdout + "\n" + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
        artifact = "catalog.json" if command == "docs generate" else "run_results.json"
        return json.loads((Path(self.directory.name) / artifact).read_text())

    def test_docs_catalog_covers_all_models_columns_and_sources(self):
        self.dbt()
        catalog = self.dbt("docs generate")
        manifest = json.loads((Path(self.directory.name) / "manifest.json").read_text())
        self.assertFalse(catalog["errors"])
        self.assertEqual(
            catalog["metadata"]["invocation_id"], manifest["metadata"]["invocation_id"]
        )
        models = {
            uid: node
            for uid, node in manifest["nodes"].items()
            if node["resource_type"] == "model"
        }
        self.assertEqual(len(models), 31)
        self.assertEqual(set(models), set(catalog["nodes"]))
        for uid, model in models.items():
            with self.subTest(model=model["name"]):
                self.assertTrue(model["description"].strip())
                self.assertEqual(
                    set(model["columns"]), set(catalog["nodes"][uid]["columns"])
                )
                for column in model["columns"].values():
                    self.assertTrue(column["description"].strip(), column["name"])
        self.assertEqual(sum(len(m["columns"]) for m in models.values()), 304)
        self.assertEqual(len(manifest["sources"]), 8)
        self.assertEqual(set(manifest["sources"]), set(catalog["sources"]))
        self.assertTrue(
            all(s["description"].strip() for s in manifest["sources"].values())
        )
        self.assertTrue((Path(self.directory.name) / "index.html").is_file())

    def test_independent_money_grains_statuses_type_one_customer_and_rebuild(self):
        self.dbt()
        self.assertEqual(self.psql("SELECT COUNT(*) FROM analytics.fct_orders"), "5")
        fact = json.loads(
            self.psql(
                "SELECT row_to_json(f) FROM analytics.fct_orders f WHERE order_id='a'"
            )
        )
        self.assertEqual(
            (
                fact["item_count"],
                fact["payment_count"],
                str(fact["item_amount"]),
                str(fact["gross_paid_amount"]),
            ),
            (2, 2, "30.0", "35.0"),
        )
        revenue = json.loads(
            self.psql("SELECT row_to_json(r) FROM analytics.mart_revenue_daily r")
        )
        self.assertEqual(
            (revenue["gmv"], revenue["freight_amount"], revenue["gross_paid_amount"]),
            (37.25, 6.75, 43.25),
        )
        self.assertEqual(
            (
                revenue["eligible_order_count"],
                revenue["orders_without_items"],
                revenue["orders_without_payments"],
            ),
            (4, 2, 1),
        )
        aov = json.loads(
            self.psql("SELECT row_to_json(a) FROM analytics.mart_average_order_value a")
        )
        self.assertEqual(
            (aov["paid_order_count"], aov["average_order_value"]), (2, 21.63)
        )
        customer = json.loads(
            self.psql("SELECT row_to_json(c) FROM analytics.dim_customer c")
        )
        self.assertEqual(
            (
                customer["customer_unique_id"],
                customer["latest_customer_id"],
                customer["customer_city"],
            ),
            ("shopper", "cb", "rio"),
        )
        self.assertEqual(
            self.psql("SELECT COUNT(DISTINCT customer_key) FROM analytics.fct_orders"),
            "1",
        )
        self.dbt()
        self.assertEqual(
            json.loads(
                self.psql("SELECT row_to_json(c) FROM analytics.dim_customer c")
            ),
            customer,
        )
        self.psql(
            "UPDATE raw_staging.customers SET customer_city='recife', source_sequence=100 WHERE customer_id='ca'"
        )
        self.dbt()
        updated = json.loads(
            self.psql("SELECT row_to_json(c) FROM analytics.dim_customer c")
        )
        self.assertEqual(updated["customer_key"], customer["customer_key"])
        self.assertEqual(updated["customer_city"], "recife")
        self.assertNotEqual(updated["record_hash"], customer["record_hash"])
        with psycopg.connect(
            host="127.0.0.1",
            port=self.port,
            dbname=self.database,
            user="olist_test_reader",
            password="isolated_test_reader",
            autocommit=True,
        ) as reader:
            self.assertEqual(
                reader.execute("SELECT COUNT(*) FROM analytics.fct_orders").fetchone()[
                    0
                ],
                5,
            )
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                reader.execute("DELETE FROM analytics.stg_payments")
            self.assertFalse(
                reader.execute(
                    "SELECT has_table_privilege(current_user, 'analytics.fct_orders', 'DELETE')"
                ).fetchone()[0]
            )

    def test_missing_customer_and_orphan_children_fail_build(self):
        self.psql(
            "UPDATE raw_staging.orders SET customer_id='missing' WHERE order_id='a'"
        )
        results = self.dbt(success=False)
        self.assertTrue(any(r["status"] == "fail" for r in results["results"]))
        self.psql(
            "UPDATE raw_staging.orders SET customer_id='ca' WHERE order_id='a'; UPDATE raw_staging.order_items SET order_id='orphan' WHERE order_id='b'"
        )
        self.dbt(success=False)

    def test_empty_sources_and_no_paid_orders_keep_aov_null(self):
        self.psql("UPDATE raw_staging.order_payments SET payment_value=0")
        self.dbt()
        self.assertEqual(
            self.psql(
                "SELECT average_order_value IS NULL FROM analytics.mart_average_order_value"
            ),
            "t",
        )
        self.psql(
            "TRUNCATE raw_staging.customers, raw_staging.orders, raw_staging.order_items, raw_staging.order_payments, raw_staging.products, raw_staging.sellers, raw_staging.geolocation, raw_staging.order_reviews"
        )
        self.dbt()
        self.assertEqual(
            self.psql("SELECT COUNT(*) FROM analytics.mart_revenue_daily"), "0"
        )
        for model in (
            "fct_order_items",
            "mart_product_daily",
            "mart_seller_daily",
            "fct_payments",
            "mart_payment_type_daily",
            "fct_delivery",
            "mart_delivery_daily",
            "dim_date",
            "dim_location",
            "dim_customer",
            "dim_product",
            "dim_seller",
            "int_geolocation_by_zip",
            "stg_reviews",
            "fct_reviews",
            "mart_review_daily",
            "mart_customer_value",
            "mart_repeat_purchase_daily",
            "mart_order_status_daily",
        ):
            self.assertEqual(self.psql(f"SELECT COUNT(*) FROM analytics.{model}"), "0")

    def test_reconciliation_detects_a_corrupted_fact(self):
        self.dbt()
        self.psql(
            "CREATE OR REPLACE VIEW analytics.int_payment_summary AS SELECT order_id, COUNT(*) AS payment_count, SUM(payment_value)*2 AS gross_paid_amount FROM analytics.stg_payments GROUP BY order_id"
        )
        results = self.dbt("test", success=False)
        self.assertTrue(
            any(
                r["unique_id"].endswith("fact_reconciliation") and r["status"] == "fail"
                for r in results["results"]
            )
        )

    def test_item_fact_and_daily_marts_preserve_money_and_distinct_group_orders(self):
        self.dbt()
        self.assertEqual(
            self.psql("SELECT COUNT(*) FROM analytics.fct_order_items"), "4"
        )
        for entity in ("product", "seller"):
            self.assertEqual(
                self.psql(
                    f"SELECT total_item_count,eligible_item_count,eligible_order_count,gmv,freight_amount FROM analytics.mart_{entity}_daily"
                ),
                "4|3|2|37.25|6.75",
            )
        with psycopg.connect(
            host="127.0.0.1",
            port=self.port,
            dbname=self.database,
            user="olist_test_loader",
            password="isolated_test_loader",
            autocommit=True,
        ) as connection:
            for entity in ("product", "seller"):
                merge_rows(
                    connection,
                    self.contract,
                    table=entity + "s",
                    run_id="extra",
                    rows=[valid_row(entity + "s", "other_" + entity, 10)],
                    request={"fixture": "multi_dimension"},
                    silver_counts={
                        "input_rows": 1,
                        "valid_rows": 1,
                        "rejected_rows": 0,
                        "deferred_rows": 0,
                        "duplicates_rows": 0,
                        "warning_rows": 0,
                    },
                )
        self.psql("""UPDATE raw_staging.order_items SET product_id='other_product',seller_id='other_seller'
                WHERE order_id='a' AND order_item_id=2;
            UPDATE raw_staging.order_items SET seller_id='other_seller' WHERE order_id='b';
            UPDATE raw_staging.orders SET order_purchase_date='2020-01-02'
                WHERE order_id='b';
            UPDATE raw_staging.orders SET order_status='unavailable' WHERE order_id='cancel';""")
        self.dbt()
        self.assertEqual(
            self.psql(
                "SELECT product_id,order_purchase_date,eligible_order_count,gmv,freight_amount FROM analytics.mart_product_daily ORDER BY product_id,order_purchase_date"
            ),
            "other_product|2019-12-31|1|20.00|3.00\nproduct|2019-12-31|1|10.00|2.00\nproduct|2020-01-02|1|7.25|1.75",
        )
        self.assertEqual(
            self.psql(
                "SELECT seller_id,order_purchase_date,eligible_order_count,gmv,freight_amount FROM analytics.mart_seller_daily ORDER BY seller_id,order_purchase_date"
            ),
            "other_seller|2019-12-31|1|20.00|3.00\nother_seller|2020-01-02|1|7.25|1.75\nseller|2019-12-31|1|10.00|2.00",
        )
        self.assertEqual(
            self.psql(
                "SELECT SUM(gmv),SUM(freight_amount) FROM analytics.mart_seller_daily"
            ),
            "37.25|6.75",
        )
        self.psql("""CREATE OR REPLACE VIEW analytics.mart_product_daily AS
            SELECT order_purchase_date, product_id, product_key,
                COUNT(*) AS total_item_count,
                COUNT(*) FILTER (WHERE is_metric_eligible) AS eligible_item_count,
                COUNT(DISTINCT order_id) FILTER (WHERE is_metric_eligible) AS eligible_order_count,
                COALESCE(SUM(price) FILTER (WHERE is_metric_eligible),0::numeric) + 1 AS gmv,
                COALESCE(SUM(freight_value) FILTER (WHERE is_metric_eligible),0::numeric) AS freight_amount
            FROM analytics.fct_order_items GROUP BY order_purchase_date,product_id,product_key;""")
        results = self.dbt("test", success=False)
        self.assertTrue(
            any(
                r["unique_id"].endswith("item_mart_reconciliation")
                and r["status"] == "fail"
                for r in results["results"]
            )
        )

    def test_missing_product_or_seller_fails_without_dropping_item_rows(self):
        for entity in ("product", "seller"):
            with self.subTest(entity=entity):
                self.psql(
                    f"UPDATE raw_staging.order_items SET {entity}_id='missing' WHERE order_id='a'"
                )
                results = self.dbt(success=False)
                self.assertEqual(
                    self.psql("SELECT COUNT(*) FROM analytics.fct_order_items"), "4"
                )
                self.assertTrue(
                    any(
                        r["unique_id"].startswith(
                            f"test.olist_analytics.not_null_fct_order_items_{entity}_key"
                        )
                        and r["status"] == "fail"
                        for r in results["results"]
                    )
                )
                self.psql(
                    f"UPDATE raw_staging.order_items SET {entity}_id='{entity}' WHERE order_id='a'"
                )

    def test_payment_entries_types_installments_dates_and_exact_money(self):
        self.dbt()
        self.assertEqual(self.psql("SELECT COUNT(*) FROM analytics.fct_payments"), "5")
        self.assertEqual(
            self.psql(
                "SELECT total_payment_count,eligible_payment_count,eligible_order_count,gross_paid_amount FROM analytics.mart_payment_type_daily"
            ),
            "5|4|3|43.25",
        )
        self.psql("""UPDATE raw_staging.order_payments SET payment_type='voucher',payment_installments=3
                WHERE order_id='a' AND payment_sequential=2;
            UPDATE raw_staging.order_payments SET payment_type='boleto' WHERE order_id='cancel';
            UPDATE raw_staging.order_payments SET payment_type='not_defined' WHERE order_id='zero';
            UPDATE raw_staging.orders SET order_purchase_date='2020-01-02' WHERE order_id='b';
            UPDATE raw_staging.orders SET order_status='unavailable' WHERE order_id='cancel';""")
        self.dbt()
        self.assertEqual(
            self.psql(
                "SELECT payment_type,order_purchase_date,total_payment_count,eligible_payment_count,eligible_order_count,gross_paid_amount FROM analytics.mart_payment_type_daily ORDER BY payment_type,order_purchase_date"
            ),
            "boleto|2019-12-31|1|0|0|0\ncredit_card|2019-12-31|1|1|1|15.00\ncredit_card|2020-01-02|1|1|1|8.25\nnot_defined|2019-12-31|1|1|1|0.00\nvoucher|2019-12-31|1|1|1|20.00",
        )
        self.assertEqual(
            self.psql(
                "SELECT payment_sequential,payment_installments,payment_value FROM analytics.fct_payments WHERE order_id='a' ORDER BY payment_sequential"
            ),
            "1|1|15.00\n2|3|20.00",
        )
        self.assertEqual(
            self.psql(
                "SELECT SUM(gross_paid_amount),SUM(eligible_order_count) FROM analytics.mart_payment_type_daily"
            ),
            "43.25|4",
        )
        self.assertEqual(
            self.psql(
                "SELECT SUM(paid_order_count) FROM analytics.mart_average_order_value"
            ),
            "2",
        )
        self.psql("""CREATE OR REPLACE VIEW analytics.fct_payments AS
            SELECT p.order_id,p.payment_sequential,o.customer_key,o.order_purchase_date,
                o.order_status,o.is_metric_eligible,p.payment_type,p.payment_installments,
                (p.payment_value*p.payment_installments)::numeric(18,2) AS payment_value,
                o.order_purchase_date_key
            FROM analytics.stg_payments p LEFT JOIN analytics.fct_orders o USING (order_id);""")
        results = self.dbt("test", success=False)
        for test in ("payment_fact_reconciliation", "payment_type_reconciliation"):
            self.assertTrue(
                any(
                    r["unique_id"].endswith(test) and r["status"] == "fail"
                    for r in results["results"]
                )
            )

    def test_missing_payment_order_fails_without_dropping_payment_rows(self):
        self.dbt()
        self.psql(
            "UPDATE raw_staging.order_payments SET order_id='orphan' WHERE order_id='b'"
        )
        results = self.dbt("test", success=False)
        self.assertEqual(self.psql("SELECT COUNT(*) FROM analytics.fct_payments"), "5")
        self.assertTrue(
            any(
                r["unique_id"].startswith(
                    "test.olist_analytics.not_null_fct_payments_order_purchase_date"
                )
                and r["status"] == "fail"
                for r in results["results"]
            )
        )
        self.dbt(success=False)

    def test_delivery_calendar_lateness_utc_dst_and_incomplete_timelines(self):
        self.psql("""UPDATE raw_staging.orders SET
            order_purchase_timestamp=CASE WHEN order_id='b' THEN '2018-11-03T15:00:00Z'::timestamptz ELSE '2018-01-01T02:00:00Z'::timestamptz END,
            order_purchase_date=CASE WHEN order_id='b' THEN '2018-11-03'::date ELSE '2018-01-01'::date END,
            order_estimated_delivery_date=CASE WHEN order_id='b' THEN '2018-11-04T02:00:00Z'::timestamptz ELSE '2018-01-03T02:00:00Z'::timestamptz END,
            order_status=CASE WHEN order_id='cancel' THEN 'canceled' ELSE 'delivered' END,
            order_delivered_customer_date=CASE order_id
                WHEN 'a' THEN '2018-01-04T01:30:00Z'::timestamptz
                WHEN 'b' THEN '2018-11-04T14:00:00Z'::timestamptz
                WHEN 'pending' THEN NULL ELSE '2018-01-04T02:00:00Z'::timestamptz END,
            order_delivered_carrier_date=CASE order_id
                WHEN 'a' THEN '2018-01-02T02:00:00Z'::timestamptz
                WHEN 'zero' THEN '2018-01-05T02:00:00Z'::timestamptz ELSE NULL END;""")
        self.dbt()
        with psycopg.connect(
            host="127.0.0.1",
            port=self.port,
            dbname=self.database,
            user="olist_test_reader",
            password="isolated_test_reader",
            options="-c timezone=Asia/Tokyo",
            autocommit=True,
        ) as reader:
            self.assertEqual(
                reader.execute(
                    "SELECT delivered_date,delivery_delay_days,delivery_elapsed_hours FROM analytics.fct_delivery WHERE order_id='a'"
                ).fetchone(),
                (date(2018, 1, 3), 0, Decimal("71.5")),
            )
        self.assertEqual(self.psql("SELECT COUNT(*) FROM analytics.fct_delivery"), "5")
        self.assertEqual(
            self.psql(
                "SELECT order_id,has_delivery_timestamp,has_invalid_delivery_chronology,is_delivery_observed,delivery_delay_days,is_late_delivery FROM analytics.fct_delivery ORDER BY order_id"
            ),
            "a|t|f|t|0|f\nb|t|f|t|1|t\ncancel|t|f|f||\npending|f|f|f||\nzero|t|t|f||",
        )
        self.assertEqual(
            self.psql(
                "SELECT delivery_elapsed_hours=71.5,carrier_to_delivery_hours=47.5 FROM analytics.fct_delivery WHERE order_id='a'"
            ),
            "t|t",
        )
        self.assertEqual(
            self.psql(
                "SELECT delivery_elapsed_hours=23,carrier_to_delivery_hours IS NULL FROM analytics.fct_delivery WHERE order_id='b'"
            ),
            "t|t",
        )
        self.assertEqual(
            self.psql(
                "SELECT order_purchase_date,total_order_count,delivered_order_count,delivered_missing_timestamp_count,invalid_chronology_count,observed_delivery_count,transit_observation_count,late_delivery_count,on_time_delivery_count FROM analytics.mart_delivery_daily ORDER BY order_purchase_date"
            ),
            "2018-01-01|4|3|1|1|1|1|0|1\n2018-11-03|1|1|0|0|1|0|1|0",
        )
        self.assertEqual(
            self.psql(
                "SELECT late_delivery_rate=0,avg_delivery_hours=71.5,avg_carrier_to_delivery_hours=47.5 FROM analytics.mart_delivery_daily WHERE order_purchase_date='2018-01-01'"
            ),
            "t|t|t",
        )
        self.assertEqual(
            self.psql(
                "SELECT late_delivery_rate=1,avg_delivery_hours=23,avg_carrier_to_delivery_hours IS NULL FROM analytics.mart_delivery_daily WHERE order_purchase_date='2018-11-03'"
            ),
            "t|t|t",
        )
        self.psql(
            "UPDATE raw_staging.orders SET order_delivered_customer_date='2018-01-02T02:00:00Z',order_delivered_carrier_date=NULL WHERE order_id='a'"
        )
        self.dbt()
        self.assertEqual(
            self.psql(
                "SELECT delivery_delay_days,is_late_delivery FROM analytics.fct_delivery WHERE order_id='a'"
            ),
            "-1|f",
        )
        self.psql(
            "UPDATE raw_staging.orders SET order_delivered_carrier_date='2017-12-31T02:00:00Z' WHERE order_id='a'"
        )
        self.dbt()
        self.assertEqual(
            self.psql(
                "SELECT has_invalid_delivery_chronology,delivery_elapsed_hours IS NULL,delivery_delay_days IS NULL FROM analytics.fct_delivery WHERE order_id='a'"
            ),
            "t|t|t",
        )
        self.assertEqual(
            self.psql(
                "SELECT late_delivery_rate IS NULL,avg_delivery_hours IS NULL FROM analytics.mart_delivery_daily WHERE order_purchase_date='2018-01-01'"
            ),
            "t|t",
        )

        self.psql(
            "UPDATE raw_staging.orders SET order_delivered_customer_date='2017-12-31T02:00:00Z',order_delivered_carrier_date=NULL WHERE order_id='a'"
        )
        self.dbt("test")
        self.assertEqual(
            self.psql(
                "SELECT has_invalid_delivery_chronology,delivery_elapsed_hours IS NULL FROM analytics.fct_delivery WHERE order_id='a'"
            ),
            "t|t",
        )

    def test_customer_seller_location_keys_preserve_source_rows_and_allow_unmatched_zips(
        self,
    ):
        groups = {
            "geolocation": [
                valid_row("geolocation", "1", 1),
                valid_row("geolocation", "2", 2),
                valid_row("geolocation", "3", 3)
                | {"geolocation_zip_code_prefix": "00456"},
            ],
            "customers": [
                valid_row("customers", "unmatched", 3)
                | {
                    "customer_unique_id": "unmatched",
                    "customer_zip_code_prefix": "99999",
                },
                valid_row("customers", "invalid", 4)
                | {"customer_unique_id": "invalid", "customer_zip_code_prefix": "123"},
                valid_row("customers", "blank", 5)
                | {"customer_unique_id": "blank", "customer_zip_code_prefix": None},
            ],
            "sellers": [
                valid_row("sellers", "unmatched", 2)
                | {"seller_zip_code_prefix": "99999"},
                valid_row("sellers", "invalid", 3) | {"seller_zip_code_prefix": "123"},
                valid_row("sellers", "blank", 4) | {"seller_zip_code_prefix": None},
            ],
        }
        with psycopg.connect(
            host="127.0.0.1",
            port=self.port,
            dbname=self.database,
            user="olist_test_loader",
            password="isolated_test_loader",
            autocommit=True,
        ) as c:
            for table, rows in groups.items():
                merge_rows(
                    c,
                    self.contract,
                    table=table,
                    run_id="locations",
                    rows=rows,
                    request={"fixture": "locations"},
                    silver_counts=dict(
                        input_rows=3,
                        valid_rows=3,
                        rejected_rows=0,
                        deferred_rows=0,
                        duplicates_rows=0,
                        warning_rows=0,
                    ),
                )
        self.psql(
            "UPDATE raw_staging.customers SET customer_zip_code_prefix='00456' WHERE customer_id='cb'"
        )
        self.dbt()
        self.assertEqual(
            self.psql(
                "SELECT customer_key=MD5('shopper'),location_key=MD5('00456'),latest_customer_id FROM analytics.dim_customer WHERE customer_unique_id='shopper'"
            ),
            "t|t|cb",
        )
        self.assertEqual(
            self.psql(
                "SELECT seller_key=MD5('seller'),location_key=MD5('00123') FROM analytics.dim_seller WHERE seller_id='seller'"
            ),
            "t|t",
        )
        for model in ("dim_customer", "dim_seller"):
            self.assertEqual(
                self.psql(
                    f"SELECT COUNT(*),COUNT(location_key) FROM analytics.{model}"
                ),
                "4|1",
            )
        self.assertEqual(
            self.psql("SELECT COUNT(*) FROM analytics.fct_order_items"), "4"
        )
        self.psql(
            "UPDATE raw_staging.geolocation SET geolocation_city='other city',geolocation_state='RJ'; UPDATE raw_staging.customers SET customer_zip_code_prefix='00123' WHERE customer_id='cb'"
        )
        self.dbt()
        self.assertEqual(
            self.psql(
                "SELECT customer_key=MD5('shopper'),location_key=MD5('00123'),customer_city,customer_state FROM analytics.dim_customer WHERE customer_unique_id='shopper'"
            ),
            "t|t|rio|SP",
        )
        self.assertEqual(
            self.psql(
                "SELECT seller_city,seller_state FROM analytics.dim_seller WHERE seller_id='seller'"
            ),
            "são paulo|SP",
        )
        self.psql("""CREATE OR REPLACE VIEW analytics.dim_seller AS
            SELECT MD5(seller_id) AS seller_key,seller_id,seller_zip_code_prefix,seller_city,seller_state,
                first_seen_at AS created_at,last_seen_at AS updated_at,record_hash,MD5('00456') AS location_key
            FROM analytics.stg_sellers;""")
        results = self.dbt("test", success=False)
        self.assertTrue(
            any(
                r["unique_id"].endswith("location_key_reconciliation")
                and r["status"] == "fail"
                for r in results["results"]
            )
        )
        relationships = [
            r
            for r in results["results"]
            if "relationships_dim_seller_location_key" in r["unique_id"]
        ]
        self.assertEqual([r["status"] for r in relationships], ["pass"])
        self.psql("TRUNCATE raw_staging.geolocation")
        self.dbt()
        for model in ("dim_customer", "dim_seller"):
            self.assertEqual(
                self.psql(
                    f"SELECT COUNT(*),COUNT(location_key) FROM analytics.{model}"
                ),
                "4|0",
            )

    def test_geolocation_exact_medians_joint_modes_type1_and_corruption_gate(self):
        observations = [
            ("00123", "-3", "10", "rio", "RJ"),
            ("00123", "-2", "20", "rio", "RJ"),
            ("00123", "-1", "30", "são paulo", "SP"),
            ("00123", "0", "40", "são paulo", "SP"),
            ("00456", "-23.000000000000000001", "-46.1", None, None),
            ("00456", "-23.000000000000000002", "-46.2", "other", "XX"),
            ("00456", "-23.000000000000000001", "-46.1", None, None),
            ("00888", "10", "20", "a", "SP"),
            ("00888", "10", "20", "a", "SP"),
            ("00888", "10", "20", "a", "SP"),
            ("00888", "10", "20", "b", "RJ"),
            ("00888", "10", "20", "b", "RJ"),
            ("00888", "10", "20", "c", "RJ"),
            ("00888", "10", "20", "c", "RJ"),
            ("00999", "23.000000000000000001", "46.000000000000000001", "a", "SP"),
            ("00999", "23.000000000000000002", "46.000000000000000002", "a", "SP"),
        ]
        rows = [
            valid_row("geolocation", str(i), i)
            | dict(
                geolocation_zip_code_prefix=zip_code,
                geolocation_lat=Decimal(lat),
                geolocation_lng=Decimal(lng),
                geolocation_city=city,
                geolocation_state=state,
                warning_rule_ids=[]
                if state in ("SP", "RJ")
                else ["invalid_geolocation_state"],
            )
            for i, (zip_code, lat, lng, city, state) in enumerate(observations, 1)
        ]
        with psycopg.connect(
            host="127.0.0.1",
            port=self.port,
            dbname=self.database,
            user="olist_test_loader",
            password="isolated_test_loader",
            autocommit=True,
        ) as c:
            merge_rows(
                c,
                self.contract,
                table="geolocation",
                run_id="observations",
                rows=rows,
                request={"fixture": "observations"},
                silver_counts=dict(
                    input_rows=16,
                    valid_rows=16,
                    rejected_rows=0,
                    deferred_rows=0,
                    duplicates_rows=0,
                    warning_rows=3,
                ),
            )
        self.dbt()
        self.assertEqual(
            self.psql("SELECT COUNT(*),SUM(sample_count) FROM analytics.dim_location"),
            "4|16",
        )
        self.assertEqual(
            self.psql("""SELECT latitude=-1.5,longitude=25,city,state,sample_count,modal_sample_count
            FROM analytics.dim_location WHERE zip_code_prefix='00123'"""),
            "t|t|rio|RJ|4|2",
        )
        self.assertEqual(
            self.psql("""SELECT latitude=-23.000000000000000001,longitude=-46.1,
            city IS NULL,state IS NULL,sample_count,modal_sample_count
            FROM analytics.dim_location WHERE zip_code_prefix='00456'"""),
            "t|t|t|t|3|0",
        )
        self.assertEqual(
            self.psql(
                "SELECT city,state,sample_count,modal_sample_count FROM analytics.dim_location WHERE zip_code_prefix='00888'"
            ),
            "a|SP|7|3",
        )
        self.assertEqual(
            self.psql(
                "SELECT latitude=23.0000000000000000015,longitude=46.0000000000000000015 FROM analytics.dim_location WHERE zip_code_prefix='00999'"
            ),
            "t|t",
        )
        original = self.psql(
            "SELECT location_key,record_hash FROM analytics.dim_location WHERE zip_code_prefix='00123'"
        )
        self.dbt()
        self.assertEqual(
            self.psql(
                "SELECT location_key,record_hash FROM analytics.dim_location WHERE zip_code_prefix='00123'"
            ),
            original,
        )
        self.psql(
            "UPDATE raw_staging.geolocation SET geolocation_lat=geolocation_lat*1.0000,geolocation_lng=geolocation_lng*1.0000"
        )
        self.dbt()
        self.assertEqual(
            self.psql(
                "SELECT location_key,record_hash FROM analytics.dim_location WHERE zip_code_prefix='00123'"
            ),
            original,
        )
        self.psql(
            "UPDATE raw_staging.geolocation SET geolocation_city='são paulo',geolocation_state='SP',geolocation_lat=10 WHERE geolocation_row_id=1"
        )
        self.dbt()
        changed = self.psql(
            "SELECT location_key,record_hash FROM analytics.dim_location WHERE zip_code_prefix='00123'"
        )
        self.assertEqual(original.split("|")[0], changed.split("|")[0])
        self.assertNotEqual(original.split("|")[1], changed.split("|")[1])
        self.assertEqual(
            self.psql(
                "SELECT latitude=-0.5,city,state,modal_sample_count FROM analytics.dim_location WHERE zip_code_prefix='00123'"
            ),
            "t|são paulo|SP|3",
        )
        self.psql("""CREATE OR REPLACE VIEW analytics.dim_location AS
            SELECT MD5(zip_code_prefix) AS location_key,zip_code_prefix,latitude+1 AS latitude,longitude,
                city,state,sample_count,modal_sample_count,created_at,updated_at,
                MD5(JSONB_BUILD_ARRAY(latitude,longitude,city,state,sample_count,modal_sample_count)::text) AS record_hash
            FROM analytics.int_geolocation_by_zip;""")
        results = self.dbt("test", success=False)
        self.assertTrue(
            any(
                r["unique_id"].endswith("geolocation_reconciliation")
                and r["status"] == "fail"
                for r in results["results"]
            )
        )
        self.psql("TRUNCATE raw_staging.geolocation")
        self.dbt()
        self.assertEqual(self.psql("SELECT COUNT(*) FROM analytics.dim_location"), "0")

    def test_date_dimension_continuity_iso_leap_day_and_fact_roles(self):
        self.psql("""UPDATE raw_staging.orders SET
            order_purchase_date='2019-12-30', order_purchase_timestamp='2019-12-30T12:00:00Z',
            order_estimated_delivery_date='2020-03-02T02:00:00Z',
            order_delivered_customer_date=CASE WHEN order_id='a' THEN '2020-03-01T02:00:00Z'::timestamptz END;
            UPDATE raw_staging.orders SET order_purchase_date='2021-01-01',
                order_purchase_timestamp='2021-01-01T12:00:00Z',
                order_estimated_delivery_date='2021-01-05T02:00:00Z' WHERE order_id='pending';
            UPDATE raw_staging.orders SET order_delivered_customer_date='2019-12-29T02:00:00Z' WHERE order_id='cancel';""")
        self.dbt()
        self.assertEqual(
            self.psql(
                "SELECT MIN(calendar_date),MAX(calendar_date),COUNT(*) FROM analytics.dim_date"
            ),
            "2019-12-28|2021-01-04|374",
        )
        self.assertEqual(
            self.psql("""SELECT date_key,calendar_year,calendar_quarter,calendar_month,day_of_month,
                iso_year,iso_week,iso_day_of_week,is_weekend FROM analytics.dim_date
                WHERE calendar_date IN ('2019-12-30','2020-02-29','2021-01-01') ORDER BY calendar_date"""),
            "20191230|2019|4|12|30|2020|1|1|f\n20200229|2020|1|2|29|2020|9|6|t\n20210101|2021|1|1|1|2020|53|5|f",
        )
        for model, count in (
            ("fct_orders", 5),
            ("fct_order_items", 4),
            ("fct_payments", 5),
            ("fct_delivery", 5),
        ):
            self.assertEqual(
                self.psql(
                    f"SELECT COUNT(*) FROM analytics.{model} WHERE order_purchase_date_key = CASE WHEN order_id='pending' THEN 20210101 ELSE 20191230 END"
                ),
                str(count),
            )
        expected = "a|20200301|20200229\nb|20200301|\ncancel|20200301|20191228\npending|20210104|\nzero|20200301|"
        query = "SELECT order_id,estimated_delivery_date_key,delivered_date_key FROM analytics.fct_delivery ORDER BY order_id"
        self.assertEqual(self.psql(query), expected)
        self.assertEqual(
            self.psql("SET TIME ZONE 'Asia/Tokyo'; " + query).splitlines()[-5:],
            expected.splitlines(),
        )
        self.assertEqual(
            self.psql(
                "SELECT is_delivery_observed,delivered_date_key FROM analytics.fct_delivery WHERE order_id='a'"
            ),
            "f|20200229",
        )

    def test_date_reconciliation_rejects_valid_but_wrong_keys_and_attributes(self):
        self.dbt()
        self.psql("""CREATE OR REPLACE VIEW analytics.fct_payments AS
            SELECT p.order_id,p.payment_sequential,o.customer_key,o.order_purchase_date,
                o.order_status,o.is_metric_eligible,p.payment_type,p.payment_installments,p.payment_value,
                20200101::integer AS order_purchase_date_key
            FROM analytics.stg_payments p LEFT JOIN analytics.fct_orders o USING (order_id);""")
        results = self.dbt("test", success=False)
        self.assertTrue(
            any(
                r["unique_id"].endswith("fact_date_reconciliation")
                and r["status"] == "fail"
                for r in results["results"]
            )
        )
        relationships = [
            r
            for r in results["results"]
            if "relationships_fct_payments_order_purchase_date_key" in r["unique_id"]
        ]
        self.assertEqual(len(relationships), 1)
        self.assertEqual(relationships[0]["status"], "pass")
        self.dbt()
        self.psql("""CREATE OR REPLACE VIEW analytics.dim_date AS
            SELECT TO_CHAR(d,'YYYYMMDD')::integer AS date_key,d AS calendar_date,
                EXTRACT(YEAR FROM d)::integer AS calendar_year,
                EXTRACT(QUARTER FROM d)::integer AS calendar_quarter,
                EXTRACT(MONTH FROM d)::integer AS calendar_month,
                EXTRACT(DAY FROM d)::integer AS day_of_month,
                9999::integer AS iso_year, EXTRACT(WEEK FROM d)::integer AS iso_week,
                EXTRACT(ISODOW FROM d)::integer AS iso_day_of_week,
                EXTRACT(ISODOW FROM d) IN (6,7) AS is_weekend
            FROM (SELECT '2019-12-31'::date+n AS d FROM generate_series(0,9) n) dates;""")
        results = self.dbt("test", success=False)
        self.assertTrue(
            any(
                r["unique_id"].endswith("date_dimension_reconciliation")
                and r["status"] == "fail"
                for r in results["results"]
            )
        )

    def test_delivery_reconciliation_detects_a_corrupted_duration(self):
        self.psql("""UPDATE raw_staging.orders SET order_status='delivered',
            order_delivered_customer_date=order_purchase_timestamp+INTERVAL '48 hours'
            WHERE order_id='a';""")
        self.dbt()
        self.psql("""CREATE OR REPLACE VIEW analytics.fct_delivery AS
            SELECT o.order_id,f.customer_key,o.order_status,o.order_purchase_date,
                o.order_purchase_timestamp,o.order_approved_at,o.order_delivered_carrier_date,
                o.order_delivered_customer_date,o.order_estimated_delivery_date,
                (o.order_estimated_delivery_date AT TIME ZONE 'America/Sao_Paulo')::date AS estimated_delivery_date,
                (o.order_delivered_customer_date AT TIME ZONE 'America/Sao_Paulo')::date AS delivered_date,
                o.order_delivered_customer_date IS NOT NULL AS has_delivery_timestamp,
                FALSE AS has_invalid_delivery_chronology,
                o.order_status='delivered' AND o.order_delivered_customer_date IS NOT NULL AS is_delivery_observed,
                CASE WHEN o.order_status='delivered' THEN
                    (o.order_delivered_customer_date AT TIME ZONE 'America/Sao_Paulo')::date - (o.order_estimated_delivery_date AT TIME ZONE 'America/Sao_Paulo')::date END AS delivery_delay_days,
                CASE WHEN o.order_status='delivered' THEN
                    (o.order_delivered_customer_date AT TIME ZONE 'America/Sao_Paulo')::date > (o.order_estimated_delivery_date AT TIME ZONE 'America/Sao_Paulo')::date END AS is_late_delivery,
                CASE WHEN o.order_status='delivered' THEN 999::numeric END AS delivery_elapsed_hours,
                NULL::numeric AS carrier_to_delivery_hours,
                f.order_purchase_date_key, ed.date_key AS estimated_delivery_date_key,
                dd.date_key AS delivered_date_key
            FROM analytics.stg_orders o LEFT JOIN analytics.fct_orders f USING (order_id)
            LEFT JOIN analytics.dim_date ed ON ed.calendar_date=(o.order_estimated_delivery_date AT TIME ZONE 'America/Sao_Paulo')::date
            LEFT JOIN analytics.dim_date dd ON dd.calendar_date=(o.order_delivered_customer_date AT TIME ZONE 'America/Sao_Paulo')::date;""")
        results = self.dbt("test", success=False)
        for test in ("delivery_fact_reconciliation", "delivery_metric_reconciliation"):
            self.assertTrue(
                any(
                    r["unique_id"].endswith(test) and r["status"] == "fail"
                    for r in results["results"]
                )
            )

    def load_review_fixture(self):
        rows = []
        for sequence, (
            review,
            order,
            score,
            instant,
            title,
            message,
            invalid,
        ) in enumerate(
            (
                ("shared", "a", 5, "2018-11-04T02:10:00+00:00", "Title", None, False),
                ("shared", "b", 1, "2018-11-04T02:20:00+00:00", None, None, False),
                (
                    "second",
                    "a",
                    3,
                    "2018-11-04T02:30:00+00:00",
                    None,
                    "Mensagem",
                    False,
                ),
                ("third", "a", 1, "2018-11-04T03:30:00+00:00", "Both", "Text", False),
                (
                    "cancelled",
                    "cancel",
                    4,
                    "2018-11-05T03:30:00+00:00",
                    None,
                    None,
                    True,
                ),
            ),
            1,
        ):
            created = datetime.fromisoformat(instant)
            rows.append(
                valid_row("order_reviews", review, sequence)
                | {
                    "order_id": order,
                    "review_score": score,
                    "review_creation_date": created,
                    "review_answer_timestamp": created
                    + timedelta(hours=-1 if invalid else 1),
                    "review_comment_title": title,
                    "review_comment_message": message,
                    "warning_rule_ids": ["review_answer_before_creation"]
                    if invalid
                    else [],
                }
            )
        with psycopg.connect(
            host="127.0.0.1",
            port=self.port,
            dbname=self.database,
            user="olist_test_loader",
            password="isolated_test_loader",
            autocommit=True,
        ) as connection:
            merge_rows(
                connection,
                self.contract,
                table="order_reviews",
                run_id="reviews",
                rows=rows,
                request={"fixture": "review-analytics"},
                silver_counts={
                    "input_rows": 5,
                    "valid_rows": 5,
                    "rejected_rows": 0,
                    "deferred_rows": 0,
                    "duplicates_rows": 0,
                    "warning_rows": 1,
                },
            )

    def test_review_grain_scores_local_dates_comments_and_weighted_metrics(self):
        self.load_review_fixture()
        self.dbt()
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*),COUNT(DISTINCT review_id),COUNT(DISTINCT order_id),SUM(review_score) FROM analytics.fct_reviews"
            ),
            "5|4|3|14",
        )
        self.assertEqual(
            self.psql("""SELECT review_creation_date,review_count,reviewed_order_count,score_sum,
            ROUND(average_score,2),low_score_count,ROUND(low_score_rate,6),commented_review_count,ROUND(comment_rate,6)
            FROM analytics.mart_review_daily ORDER BY review_creation_date"""),
            "2018-11-03|3|2|9|3.00|1|0.333333|2|0.666667\n"
            "2018-11-04|1|1|1|1.00|1|1.000000|1|1.000000\n"
            "2018-11-05|1|1|4|4.00|0|0.000000|0|0.000000",
        )
        self.assertEqual(
            self.psql("""SELECT ROUND(SUM(score_sum)::numeric/SUM(review_count),2),
            SUM(reviewed_order_count),SUM(low_score_count),SUM(commented_review_count)
            FROM analytics.mart_review_daily"""),
            "2.80|4|2|3",
        )
        query = "SELECT review_id,order_id,review_creation_date,review_creation_date_key FROM analytics.fct_reviews ORDER BY review_id,order_id"
        expected = "cancelled|cancel|2018-11-05|20181105\nsecond|a|2018-11-03|20181103\nshared|a|2018-11-03|20181103\nshared|b|2018-11-03|20181103\nthird|a|2018-11-04|20181104"
        self.assertEqual(self.psql(query), expected)
        self.assertEqual(
            self.psql("SET TIME ZONE 'Asia/Tokyo'; " + query).splitlines()[-5:],
            expected.splitlines(),
        )
        self.assertEqual(
            self.psql(
                "SELECT has_invalid_answer_chronology FROM analytics.fct_reviews WHERE review_id='cancelled'"
            ),
            "t",
        )
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='analytics' AND table_name='fct_reviews' AND column_name IN ('review_comment_title','review_comment_message')"
            ),
            "0",
        )
        self.assertEqual(
            self.psql("SELECT gmv,gross_paid_amount FROM analytics.mart_revenue_daily"),
            "37.25|43.25",
        )
        self.assertEqual(
            self.psql(
                "SELECT paid_order_count,average_order_value FROM analytics.mart_average_order_value"
            ),
            "2|21.63",
        )
        self.assertEqual(
            self.psql("SELECT COUNT(*) FROM analytics.fct_order_items"), "4"
        )
        self.assertEqual(self.psql("SELECT COUNT(*) FROM analytics.fct_payments"), "5")
        for model in ("stg_reviews", "fct_reviews", "mart_review_daily"):
            self.assertEqual(
                self.psql(
                    f"SELECT has_table_privilege('olist_bi_reader','analytics.{model}','SELECT')"
                ),
                "f",
            )
        self.dbt()
        self.assertEqual(self.psql(query), expected)

    def test_reviews_only_calendar_and_empty_review_sources(self):
        self.load_review_fixture()
        self.psql(
            "TRUNCATE raw_staging.orders,raw_staging.customers,raw_staging.order_items,raw_staging.order_payments"
        )
        self.dbt()
        self.assertEqual(self.psql("SELECT COUNT(*) FROM analytics.fct_orders"), "0")
        self.assertEqual(self.psql("SELECT COUNT(*) FROM analytics.fct_reviews"), "5")
        self.assertEqual(
            self.psql(
                "SELECT MIN(calendar_date),MAX(calendar_date),COUNT(*) FROM analytics.dim_date"
            ),
            "2018-11-03|2018-11-05|3",
        )
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM analytics.fct_reviews WHERE review_creation_date_key IS NULL"
            ),
            "0",
        )
        self.psql("TRUNCATE raw_staging.order_reviews")
        self.dbt()
        for model in ("stg_reviews", "fct_reviews", "mart_review_daily", "dim_date"):
            self.assertEqual(self.psql(f"SELECT COUNT(*) FROM analytics.{model}"), "0")

    def test_review_fact_reconciliation_rejects_valid_but_wrong_attributes(self):
        self.load_review_fixture()
        self.dbt()
        for change in (
            "review_score=4",
            "review_creation_date_key=20181104",
            "has_comment_title=FALSE",
        ):
            with self.subTest(change=change):
                self.psql(
                    "DROP TABLE IF EXISTS analytics.saved_review_fact CASCADE; CREATE TABLE analytics.saved_review_fact AS SELECT * FROM analytics.fct_reviews; GRANT SELECT ON analytics.saved_review_fact TO olist_test_loader;"
                )
                self.psql(
                    f"UPDATE analytics.saved_review_fact SET {change} WHERE review_id='shared' AND order_id='a'; CREATE OR REPLACE VIEW analytics.fct_reviews AS SELECT * FROM analytics.saved_review_fact;"
                )
                results = self.dbt("test", success=False)
                self.assertTrue(
                    any(
                        r["unique_id"].endswith("review_fact_reconciliation")
                        and r["status"] == "fail"
                        for r in results["results"]
                    )
                )
                generic_checks = [
                    r["status"]
                    for r in results["results"]
                    if "accepted_values_fct_reviews_review_score" in r["unique_id"]
                    or "relationships_fct_reviews_review_creation_date_key"
                    in r["unique_id"]
                ]
                self.assertEqual(generic_checks, ["pass", "pass"])
                self.dbt()

    def test_review_metric_reconciliation_rejects_coherent_wrong_averages(self):
        self.load_review_fixture()
        self.dbt()
        self.psql("""CREATE TABLE analytics.saved_review_mart AS SELECT * FROM analytics.mart_review_daily; GRANT SELECT ON analytics.saved_review_mart TO olist_test_loader;
            UPDATE analytics.saved_review_mart SET score_sum=score_sum+review_count,average_score=average_score+1 WHERE review_creation_date='2018-11-03';
            CREATE OR REPLACE VIEW analytics.mart_review_daily AS SELECT * FROM analytics.saved_review_mart;""")
        results = self.dbt("test", success=False)
        self.assertTrue(
            any(
                r["unique_id"].endswith("review_metric_reconciliation")
                and r["status"] == "fail"
                for r in results["results"]
            )
        )
        self.assertTrue(
            any(
                r["unique_id"].endswith("review_fact_reconciliation")
                and r["status"] == "pass"
                for r in results["results"]
            )
        )

    def test_dimension_keys_reject_unused_hashes_and_wrong_customer_representatives(
        self,
    ):
        with psycopg.connect(
            host="127.0.0.1",
            port=self.port,
            dbname=self.database,
            user="olist_test_loader",
            password="isolated_test_loader",
            autocommit=True,
        ) as connection:
            for table, row in (
                (
                    "customers",
                    valid_row("customers", "unused_customer", 3)
                    | {"customer_unique_id": "unused_shopper"},
                ),
                ("products", valid_row("products", "unused_product", 2)),
                ("sellers", valid_row("sellers", "unused_seller", 2)),
            ):
                merge_rows(
                    connection,
                    self.contract,
                    table=table,
                    run_id="unused_dimension",
                    rows=[row],
                    request={"fixture": "dimension_keys"},
                    silver_counts={
                        "input_rows": 1,
                        "valid_rows": 1,
                        "rejected_rows": 0,
                        "deferred_rows": 0,
                        "duplicates_rows": 0,
                        "warning_rows": 0,
                    },
                )
        self.psql(
            "UPDATE raw_staging.customers SET source_sequence=2 WHERE customer_id='ca'"
        )
        self.dbt()
        self.assertEqual(
            self.psql(
                "SELECT latest_customer_id FROM analytics.dim_customer WHERE customer_unique_id='shopper'"
            ),
            "cb",
        )
        for entity, column, business, change in (
            (
                "customer",
                "customer_unique_id",
                "unused_shopper",
                "customer_key=MD5('wrong_unused_key')",
            ),
            (
                "product",
                "product_id",
                "unused_product",
                "product_key=MD5('wrong_unused_key')",
            ),
            (
                "seller",
                "seller_id",
                "unused_seller",
                "seller_key=MD5('wrong_unused_key')",
            ),
            ("customer", "customer_unique_id", "shopper", "latest_customer_id='ca'"),
        ):
            with self.subTest(entity=entity, change=change):
                self.psql(f"""DROP TABLE IF EXISTS analytics.saved_dimension_keys CASCADE;
                    CREATE TABLE analytics.saved_dimension_keys AS SELECT * FROM analytics.dim_{entity};
                    GRANT SELECT ON analytics.saved_dimension_keys TO olist_test_loader;
                    UPDATE analytics.saved_dimension_keys SET {change} WHERE {column}='{business}';
                    CREATE OR REPLACE VIEW analytics.dim_{entity} AS SELECT * FROM analytics.saved_dimension_keys;""")
                results = self.dbt("test", success=False)
                expected_failures = {"dimension_key_reconciliation"}
                if entity == "customer" and change.startswith("customer_key="):
                    expected_failures.add("customer_value_reconciliation")
                self.assertEqual(
                    {
                        (r["unique_id"].split(".")[-1], r["status"])
                        for r in results["results"]
                        if r["status"] != "pass"
                    },
                    {(name, "fail") for name in expected_failures},
                )
                self.dbt()

    def test_synthetic_unknown_dimension_members_fail_source_membership(self):
        self.dbt()
        for entity, business in (
            ("customer", "customer_unique_id"),
            ("product", "product_id"),
        ):
            with self.subTest(entity=entity):
                self.psql(f"""DROP TABLE IF EXISTS analytics.saved_unknown_member CASCADE;
                    CREATE TABLE analytics.saved_unknown_member AS SELECT * FROM analytics.dim_{entity};
                    GRANT SELECT ON analytics.saved_unknown_member TO olist_test_loader;
                    INSERT INTO analytics.saved_unknown_member
                    SELECT (JSONB_POPULATE_RECORD(NULL::analytics.saved_unknown_member,
                        TO_JSONB(d) || JSONB_BUILD_OBJECT('{business}','__unknown__',
                            '{entity}_key',MD5('__unknown__')))).*
                    FROM analytics.dim_{entity} d LIMIT 1;
                    CREATE OR REPLACE VIEW analytics.dim_{entity} AS SELECT * FROM analytics.saved_unknown_member;""")
                results = self.dbt("test", success=False)
                expected_failures = {"dimension_key_reconciliation"}
                if entity == "customer":
                    expected_failures.add("customer_value_reconciliation")
                self.assertEqual(
                    {
                        (r["unique_id"].split(".")[-1], r["status"])
                        for r in results["results"]
                        if r["status"] != "pass"
                    },
                    {(name, "fail") for name in expected_failures},
                )
                self.dbt()

    def test_customer_value_combines_shopper_ids_and_preserves_zero_order_members(self):
        rows = [
            valid_row("customers", "idle", 3) | {"customer_unique_id": "idle_shopper"},
            valid_row("customers", "canceled_only", 4)
            | {"customer_unique_id": "canceled_shopper"},
        ]
        with psycopg.connect(
            host="127.0.0.1",
            port=self.port,
            dbname=self.database,
            user="olist_test_loader",
            password="isolated_test_loader",
            autocommit=True,
        ) as connection:
            merge_rows(
                connection,
                self.contract,
                table="customers",
                run_id="customer_marts",
                rows=rows,
                request={"fixture": "customer_marts"},
                silver_counts={
                    "input_rows": 2,
                    "valid_rows": 2,
                    "rejected_rows": 0,
                    "deferred_rows": 0,
                    "duplicates_rows": 0,
                    "warning_rows": 0,
                },
            )
        self.psql(
            "UPDATE raw_staging.orders SET customer_id='canceled_only' WHERE order_id='cancel'"
        )
        self.dbt()
        self.assertEqual(
            self.psql("SELECT COUNT(*) FROM analytics.mart_customer_value"), "3"
        )
        self.assertEqual(
            self.psql("""SELECT total_order_count,eligible_order_count,paid_order_count,
            orders_without_items,orders_without_payments,gmv,freight_amount,gross_paid_amount,
            average_order_value,is_repeat_customer FROM analytics.mart_customer_value WHERE customer_unique_id='shopper'"""),
            "4|4|2|2|1|37.25|6.75|43.25|21.63|t",
        )
        self.assertEqual(
            self.psql("""SELECT customer_unique_id,total_order_count,eligible_order_count,paid_order_count,
            gmv=0,freight_amount=0,gross_paid_amount=0,average_order_value IS NULL,
            first_eligible_purchase_timestamp IS NULL,last_eligible_purchase_timestamp IS NULL,is_repeat_customer
            FROM analytics.mart_customer_value WHERE customer_unique_id<>'shopper' ORDER BY customer_unique_id"""),
            "canceled_shopper|1|0|0|t|t|t|t|t|t|f\nidle_shopper|0|0|0|t|t|t|t|t|t|f",
        )
        self.assertEqual(
            self.psql("""SELECT eligible_order_count,first_order_count,repeat_order_count,
            purchasing_customer_count,repeat_customer_count,ROUND(repeat_order_rate,2) FROM analytics.mart_repeat_purchase_daily"""),
            "4|1|3|1|1|0.75",
        )
        self.assertEqual(
            self.psql(
                "SELECT order_status,order_count,customer_count FROM analytics.mart_order_status_daily ORDER BY order_status"
            ),
            "approved|4|1\ncanceled|1|1",
        )
        self.assertEqual(self.psql("SELECT COUNT(*) FROM analytics.fct_orders"), "5")
        for model in (
            "mart_customer_value",
            "mart_repeat_purchase_daily",
            "mart_order_status_daily",
        ):
            self.assertEqual(
                self.psql(
                    f"SELECT has_table_privilege('olist_bi_reader','analytics.{model}','SELECT')"
                ),
                "f",
            )

    def test_repeat_purchase_uses_observed_eligible_history_and_local_dates(self):
        self.psql("""UPDATE raw_staging.orders SET
            order_purchase_timestamp=CASE order_id
                WHEN 'a' THEN '2018-11-04T02:30:00Z'::timestamptz
                WHEN 'b' THEN '2018-11-04T03:30:00Z'::timestamptz
                WHEN 'pending' THEN '2018-11-04T03:30:00Z'::timestamptz
                WHEN 'zero' THEN '2018-11-05T03:30:00Z'::timestamptz
                ELSE '2018-11-03T02:30:00Z'::timestamptz END;
            UPDATE raw_staging.orders SET order_purchase_date=(order_purchase_timestamp AT TIME ZONE 'America/Sao_Paulo')::date;""")
        self.dbt()
        query = "SELECT order_purchase_date,eligible_order_count,first_order_count,repeat_order_count,purchasing_customer_count,repeat_customer_count FROM analytics.mart_repeat_purchase_daily ORDER BY order_purchase_date"
        expected = "2018-11-03|1|1|0|1|0\n2018-11-04|2|0|2|1|1\n2018-11-05|1|0|1|1|1"
        self.assertEqual(self.psql(query), expected)
        self.assertEqual(
            self.psql("SET TIME ZONE 'Asia/Tokyo'; " + query).splitlines()[-3:],
            expected.splitlines(),
        )
        self.assertEqual(
            self.psql("""SELECT first_eligible_purchase_timestamp='2018-11-04T02:30:00Z'::timestamptz,
            last_eligible_purchase_timestamp='2018-11-05T03:30:00Z'::timestamptz FROM analytics.mart_customer_value"""),
            "t|t",
        )
        row = valid_row("orders", "earlier", 10) | {
            "customer_id": "ca",
            "order_purchase_timestamp": datetime.fromisoformat(
                "2018-11-03T01:30:00+00:00"
            ),
            "order_purchase_date": "2018-11-02",
        }
        with psycopg.connect(
            host="127.0.0.1",
            port=self.port,
            dbname=self.database,
            user="olist_test_loader",
            password="isolated_test_loader",
            autocommit=True,
        ) as connection:
            merge_rows(
                connection,
                self.contract,
                table="orders",
                run_id="earlier",
                rows=[row],
                request={"fixture": "earlier"},
                silver_counts={
                    "input_rows": 1,
                    "valid_rows": 1,
                    "rejected_rows": 0,
                    "deferred_rows": 0,
                    "duplicates_rows": 0,
                    "warning_rows": 0,
                },
            )
        self.dbt()
        self.assertEqual(
            self.psql(query),
            "2018-11-02|1|1|0|1|0\n2018-11-03|1|0|1|1|1\n2018-11-04|2|0|2|1|1\n2018-11-05|1|0|1|1|1",
        )
        self.assertEqual(
            self.psql(
                "SELECT eligible_order_count,paid_order_count,average_order_value FROM analytics.mart_customer_value"
            ),
            "5|2|21.63",
        )

    def test_customer_value_keeps_unbounded_brl_sums_and_null_unpaid_aov(self):
        self.psql("""UPDATE raw_staging.order_items SET price=9000000000000000.01 WHERE order_id='a';
            UPDATE raw_staging.order_payments SET payment_value=9000000000000000.01 WHERE order_id='a';""")
        self.dbt()
        self.assertEqual(
            self.psql(
                "SELECT gmv,gross_paid_amount,paid_order_count,average_order_value FROM analytics.mart_customer_value"
            ),
            "18000000000000007.27|18000000000000008.27|2|9000000000000004.14",
        )
        self.psql("UPDATE raw_staging.order_payments SET payment_value=0")
        self.dbt()
        self.assertEqual(
            self.psql(
                "SELECT paid_order_count,gross_paid_amount=0,average_order_value IS NULL,is_repeat_customer FROM analytics.mart_customer_value"
            ),
            "0|t|t|t",
        )

    def test_customer_mart_reconciliations_reject_valid_but_wrong_metrics(self):
        self.dbt()
        for model, change, reconciliation in (
            ("mart_customer_value", "gmv=gmv+0.01", "customer_value_reconciliation"),
            (
                "mart_repeat_purchase_daily",
                "first_order_count=0,repeat_order_count=eligible_order_count,repeat_order_rate=1",
                "repeat_purchase_reconciliation",
            ),
            (
                "mart_order_status_daily",
                "order_count=order_count+1",
                "order_status_reconciliation",
            ),
        ):
            with self.subTest(model=model):
                self.psql(f"""DROP TABLE IF EXISTS analytics.saved_customer_mart CASCADE;
                    CREATE TABLE analytics.saved_customer_mart AS SELECT * FROM analytics.{model};
                    GRANT SELECT ON analytics.saved_customer_mart TO olist_test_loader;
                    UPDATE analytics.saved_customer_mart SET {change};
                    CREATE OR REPLACE VIEW analytics.{model} AS SELECT * FROM analytics.saved_customer_mart;""")
                results = self.dbt("test", success=False)
                self.assertEqual(
                    [
                        (r["unique_id"].split(".")[-1], r["status"])
                        for r in results["results"]
                        if r["status"] != "pass"
                    ],
                    [(reconciliation, "fail")],
                )
                self.dbt()

    def test_order_fact_reconciliation_rejects_valid_identity_and_time_drift(self):
        row = valid_row("customers", "other", 3) | {
            "customer_unique_id": "other_shopper"
        }
        with psycopg.connect(
            host="127.0.0.1",
            port=self.port,
            dbname=self.database,
            user="olist_test_loader",
            password="isolated_test_loader",
            autocommit=True,
        ) as connection:
            merge_rows(
                connection,
                self.contract,
                table="customers",
                run_id="other_shopper",
                rows=[row],
                request={"fixture": "fact_identity"},
                silver_counts={
                    "input_rows": 1,
                    "valid_rows": 1,
                    "rejected_rows": 0,
                    "deferred_rows": 0,
                    "duplicates_rows": 0,
                    "warning_rows": 0,
                },
            )
        self.dbt()
        for change in (
            "customer_id='cb'",
            "customer_unique_id='other_shopper'",
            "customer_key=MD5('other_shopper')",
            "order_status='processing'",
            "order_purchase_timestamp=order_purchase_timestamp+INTERVAL '1 minute'",
            "order_purchase_date=order_purchase_date+1",
            "is_metric_eligible=FALSE",
        ):
            with self.subTest(change=change):
                self.psql(f"""DROP TABLE IF EXISTS analytics.saved_order_fact CASCADE;
                    CREATE TABLE analytics.saved_order_fact AS SELECT * FROM analytics.fct_orders;
                    GRANT SELECT ON analytics.saved_order_fact TO olist_test_loader;
                    UPDATE analytics.saved_order_fact SET {change} WHERE order_id='a';
                    CREATE OR REPLACE VIEW analytics.fct_orders AS SELECT * FROM analytics.saved_order_fact;""")
                results = self.dbt("test", success=False)
                self.assertTrue(
                    any(
                        r["unique_id"].endswith(".fact_reconciliation")
                        and r["status"] == "fail"
                        for r in results["results"]
                    )
                )
                checks = [
                    r
                    for r in results["results"]
                    if r["unique_id"].startswith(
                        (
                            "test.olist_analytics.not_null_fct_orders_",
                            "test.olist_analytics.unique_fct_orders_",
                            "test.olist_analytics.relationships_fct_orders_",
                        )
                    )
                ]
                self.assertEqual(len(checks), 7)
                self.assertTrue(all(r["status"] == "pass" for r in checks))
                self.dbt()
