from __future__ import annotations

import hashlib
import json
import os
import time
from copy import deepcopy
from uuid import uuid4

import boto3
import psycopg
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from postgres_case import REPOSITORY_ROOT, PostgresIntegrationCase, require_success, run

from olist_platform.bronze.orders import ingest_orders
from olist_platform.bronze.storage import (
    ObjectStore,
)


class BronzeFixtureCase(PostgresIntegrationCase):
    requires_raw_snapshot = False
    publish_port = True

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        compose = os.environ.get(
            "OLIST_TEST_COMPOSE_FILE",
            str(REPOSITORY_ROOT / "infrastructure/docker-compose.yml"),
        )
        config = json.loads(
            require_success(
                run("docker", "compose", "-f", compose, "config", "--format", "json")
            )
        )
        cls.minio_container = f"olist-bronze-test-{uuid4().hex[:12]}"
        cls.addClassCleanup(
            lambda: require_success(
                run("docker", "rm", "--force", "--volumes", cls.minio_container)
            )
        )
        require_success(
            run(
                "docker",
                "run",
                "--detach",
                "--rm",
                "--name",
                cls.minio_container,
                "--memory",
                "512m",
                "--publish",
                "127.0.0.1::9000",
                "--mount",
                "type=volume,destination=/data",
                "--env",
                "MINIO_ROOT_USER=bronze_test_admin",
                "--env",
                "MINIO_ROOT_PASSWORD=isolated_bronze_password",
                config["services"]["minio"]["image"],
            )
        )
        address = require_success(
            run("docker", "port", cls.minio_container, "9000/tcp")
        )
        cls.endpoint = f"http://127.0.0.1:{address.rsplit(':', 1)[1]}"
        cls.client = boto3.client(
            "s3",
            endpoint_url=cls.endpoint,
            region_name="us-east-1",
            aws_access_key_id="bronze_test_admin",
            aws_secret_access_key="isolated_bronze_password",
            config=Config(
                s3={"addressing_style": "path"},
                retries={"max_attempts": 0},
                connect_timeout=1,
                read_timeout=2,
            ),
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                cls.client.list_buckets()
                break
            except (ClientError, BotoCoreError):
                time.sleep(0.5)
        else:
            raise RuntimeError("Test MinIO did not become ready")

    def setUp(self) -> None:
        super().setUp()
        self.contract = deepcopy(self.contract)
        self.contract["dataset_snapshot_id"] = "bronze-orders-fixture-v1"
        fixture = REPOSITORY_ROOT / "tests/fixtures/bronze/orders.sql"
        self.psql(fixture.read_text())
        digest = hashlib.sha256(fixture.read_bytes()).hexdigest()
        self.psql(f"""
            INSERT INTO audit.source_seed_runs
                (snapshot_id, manifest_sha256, seed_batch_id, seed_source_updated_at, row_counts, status)
            VALUES ('bronze-orders-fixture-v1', '{digest}', 'bronze_fixture',
                    '2020-01-04T00:00:00Z', '{{"orders":2}}', 'SUCCESS');
        """)
        self.store = ObjectStore(self.client, f"bronze-test-{uuid4().hex[:12]}")
        self.store.initialize()

    def connection(self):
        return psycopg.connect(
            host="127.0.0.1",
            port=self.port,
            dbname=self.database,
            user="olist_test_loader",
            password="isolated_test_loader",
            autocommit=True,
        )

    def ingest(self, run_id="first", store=None, page_size=1):
        with self.connection() as connection:
            return ingest_orders(
                connection,
                store or self.store,
                self.contract,
                run_id=run_id,
                endpoint=self.endpoint,
                page_size=page_size,
            )

    def rows(self, manifest):
        return [
            json.loads(line)
            for part in manifest["objects"]
            for line in self.store.read(part["key"]).splitlines()
        ]

    def state(self):
        return self.psql(
            "SELECT row_to_json(c)::text FROM audit.bronze_orders_control c;"
        )

    def failure_store(self, suffix):
        base = self.store

        class FailOnce(ObjectStore):
            def put_immutable(self, key, body):
                if key.endswith(suffix):
                    raise RuntimeError("injected upload failure")
                return super().put_immutable(key, body)

        return FailOnce(base.client, base.bucket)
