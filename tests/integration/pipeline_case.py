from __future__ import annotations

import os
from unittest.mock import patch

from bronze_case import BronzeFixtureCase
from postgres_case import REPOSITORY_ROOT

from olist_platform.bronze.extraction import ingest_source
from olist_platform.deferred.lifecycle import register_silver, resolve_snapshot
from olist_platform.pipeline.control import (
    TABLES,
    parent_run_id,
    task,
)
from olist_platform.pipeline.incremental import freeze
from olist_platform.silver.batch import process_batch
from olist_platform.silver.orders import spark_session
from olist_platform.warehouse.loading import load_silver


class CorePipelineCase(BronzeFixtureCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)

    def setUp(self):
        super().setUp()
        for name in ("customers.sql", "order_children.sql"):
            self.psql((REPOSITORY_ROOT / "tests/fixtures/bronze" / name).read_text())
        self.psql("""DELETE FROM olist.orders WHERE order_id!='order_a';
            DELETE FROM olist.order_items WHERE order_id!='order_a';
            DELETE FROM olist.order_payments WHERE order_id!='order_a';
            UPDATE olist.customers SET source_updated_at='2020-01-10T00:00:00Z';
            UPDATE olist.orders SET source_updated_at='2020-01-10T00:00:00Z';
            UPDATE olist.order_items SET source_updated_at='2020-01-10T00:00:00Z';
            UPDATE olist.order_payments SET source_updated_at='2020-01-10T00:00:00Z';""")
        self.psql("""INSERT INTO olist.customers
            (customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,
             customer_state,source_sequence,source_updated_at,source_operation,source_batch_id)
            VALUES ('customer_old','shopper_old','00123','Sao Paulo','SP',50,
                    '2020-01-01T00:00:00Z','INSERT','old_parent');
            INSERT INTO olist.orders
            (order_id,customer_id,order_status,order_purchase_timestamp,order_estimated_delivery_date,
             source_sequence,source_updated_at,source_operation,source_batch_id)
            VALUES ('order_old','customer_old','created','2018-01-01','2018-01-10',50,
                    '2020-01-01T00:00:00Z','INSERT','old_parent');""")

    def environment(self, mode="incremental"):
        return patch.dict(
            os.environ,
            {
                "DATASET_MODE": mode,
                "PGHOST": "127.0.0.1",
                "PGPORT": str(self.port),
                "PGDATABASE": self.database,
                "PGUSER": "olist_test_loader",
                "PGPASSWORD": "isolated_test_loader",
                "DBT_ENV_SECRET_PASSWORD": "isolated_test_loader",
                "DBT_READER_USER": "olist_test_reader",
                "BRONZE_S3_ENDPOINT": self.endpoint,
                "BRONZE_S3_BUCKET": self.store.bucket,
            },
        )

    def frozen(self, c, identity, attempt):
        task(c, identity, attempt, "source.freeze", "RUNNING")
        result = freeze(c, self.store, self.contract, identity, attempt, self.endpoint)
        task(c, identity, attempt, "source.freeze", "SUCCESS")
        return result

    def process(self, c, identity, attempt, mode="incremental"):
        parents = {}
        published = {}

        def event(name, function):
            task(c, identity, attempt, name, "RUNNING")
            result = function()
            task(c, identity, attempt, name, "SUCCESS")
            return result

        for parent in (False,) if mode == "full" else (True, False):
            for table in TABLES[:2] if parent else TABLES:
                run_id = parent_run_id(identity) if parent else identity
                prefix = f"{table}.{'parent.' if parent else ''}"
                bronze = event(
                    prefix + "bronze.extract",
                    lambda: ingest_source(
                        c,
                        self.store,
                        self.contract,
                        table=table,
                        run_id=run_id,
                        endpoint=self.endpoint,
                        **(
                            {"full_snapshot": table != "customers"}
                            if mode == "full"
                            else {
                                "pipeline_run_id": identity,
                                "pipeline_parent": parent,
                                "pipeline_mode": mode,
                            }
                        ),
                    ),
                )["manifest"]
                dependency = (
                    {"customer_snapshot": parents["customers"]}
                    if table == "orders"
                    else {"order_snapshot": parents["orders"]}
                    if table in TABLES[2:]
                    else {}
                )
                silver = event(
                    prefix + "silver.process",
                    lambda: process_batch(
                        self.spark,
                        self.store,
                        self.contract,
                        table=table,
                        run_id=run_id,
                        bronze_prefix=bronze["prefix"],
                        **dependency,
                    ),
                )["manifest"]
                if parent:
                    parents[table] = silver["prefix"]
                    continue
                if mode == "full":
                    parents[table] = silver["prefix"]
                published[table] = (bronze, silver)
                event(
                    prefix + "warehouse.load",
                    lambda: load_silver(
                        self.spark,
                        c,
                        self.store,
                        self.contract,
                        table=table,
                        run_id=identity,
                        silver_prefix=silver["prefix"],
                    ),
                )
                if table != "customers":
                    event(
                        prefix + "deferred.register",
                        lambda: register_silver(
                            self.spark,
                            c,
                            self.store,
                            self.contract,
                            table=table,
                            run_id=identity,
                            silver_prefix=silver["prefix"],
                        ),
                    )
                    event(
                        prefix + "deferred.resolve",
                        lambda: resolve_snapshot(
                            self.spark,
                            c,
                            self.store,
                            self.contract,
                            table=table,
                            run_id=identity,
                            origin_run_id=identity if mode == "backfill" else None,
                            parent_snapshot=parents[
                                "customers" if table == "orders" else "orders"
                            ],
                        ),
                    )
        return published
