from __future__ import annotations

import os
from unittest.mock import patch

from bronze_case import BronzeFixtureCase
from postgres_case import REPOSITORY_ROOT

from olist_platform.bronze.extraction import ingest_source
from olist_platform.deferred.lifecycle import register_silver, resolve_snapshot
from olist_platform.pipeline.control import (
    DEPENDENTS,
    FULL_SNAPSHOTS,
    PARENTS,
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
        self.psql("""INSERT INTO olist.products (product_id, source_updated_at, source_operation, source_batch_id)
            VALUES ('product','2019-01-01','INSERT','parent_fixture');
            INSERT INTO olist.sellers (seller_id,seller_city,seller_state,seller_zip_code_prefix,source_updated_at,source_operation,source_batch_id)
            VALUES ('seller','Sao Paulo','SP','00123','2019-01-01','INSERT','parent_fixture');
            INSERT INTO olist.geolocation (geolocation_row_id,geolocation_zip_code_prefix,geolocation_lat,geolocation_lng,geolocation_city,geolocation_state,source_updated_at,source_operation,source_batch_id)
            VALUES (1,'00123',-23.5,-46.5,'São Paulo','SP','2019-01-01','INSERT','parent_fixture');""")

        self.psql("""INSERT INTO olist.order_reviews
            (review_id,order_id,review_score,review_creation_date,review_answer_timestamp,
             source_updated_at,source_operation,source_batch_id)
            VALUES ('shared','order_a',5,'2020-01-01','2020-01-02','2019-01-01','INSERT','review_fixture'),
                   ('shared','order_old',4,'2020-01-01','2020-01-02','2019-01-01','INSERT','review_fixture'),
                   ('second','order_a',3,'2020-01-01','2020-01-02','2019-01-01','INSERT','review_fixture');""")

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
        if mode == "full":
            self.frozen(c, identity, attempt)

        def event(name, function):
            task(c, identity, attempt, name, "RUNNING")
            result = function()
            task(c, identity, attempt, name, "SUCCESS")
            return result

        for parent in (False,) if mode == "full" else (True, False):
            for table in (
                PARENTS
                if parent
                else (
                    (
                        "customers",
                        "products",
                        "sellers",
                        "geolocation",
                        "orders",
                        "order_items",
                        "order_payments",
                        "order_reviews",
                    )
                    if mode == "full"
                    else TABLES
                )
            ):
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
                            {
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
                    if table in ("order_items", "order_payments", "order_reviews")
                    else {}
                )
                if table == "order_items":
                    dependency.update(
                        product_snapshot=parents["products"],
                        seller_snapshot=parents["sellers"],
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
                    if table not in FULL_SNAPSHOTS:
                        continue
                if mode == "full":
                    parents[table] = silver["prefix"]
                published[table] = (bronze, silver)
                event(
                    f"{table}.warehouse.load",
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
                if table in DEPENDENTS:
                    event(
                        f"{table}.deferred.register",
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
                        f"{table}.deferred.resolve",
                        lambda: resolve_snapshot(
                            self.spark,
                            c,
                            self.store,
                            self.contract,
                            table=table,
                            run_id=identity,
                            origin_run_id=run_id if mode == "backfill" else None,
                            parent_snapshot=parents[
                                "customers" if table == "orders" else "orders"
                            ],
                            **(
                                {
                                    "product_snapshot": parents["products"],
                                    "seller_snapshot": parents["sellers"],
                                }
                                if table == "order_items"
                                else {}
                            ),
                        ),
                    )
        return published
