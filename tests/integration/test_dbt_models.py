from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import date
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
                "analytics.fct_payments",
                "analytics.dim_product",
                "analytics.dim_seller",
                "analytics.fct_order_items",
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
                command,
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
        return json.loads((Path(self.directory.name) / "run_results.json").read_text())

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
            "TRUNCATE raw_staging.customers, raw_staging.orders, raw_staging.order_items, raw_staging.order_payments, raw_staging.products, raw_staging.sellers"
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
                (p.payment_value*p.payment_installments)::numeric(18,2) AS payment_value
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
