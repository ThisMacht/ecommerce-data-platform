"""Disposable PostgreSQL harness shared by source and simulator tests."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
import unittest
from pathlib import Path
from uuid import uuid4

from olist_platform.raw.contracts import load_contract, validate_contract
from olist_platform.raw.manifest import load_manifest, verify_snapshot

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def run(*arguments: str, sql: str | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        arguments, input=sql, capture_output=True, text=True, timeout=240, check=False
    )


def require_success(result: subprocess.CompletedProcess) -> str:
    if result.returncode:
        raise RuntimeError(f"Command failed:\n{result.stdout}\n{result.stderr}")
    return result.stdout.strip()


class PostgresIntegrationCase(unittest.TestCase):
    requires_raw_snapshot = True
    publish_port = False

    @classmethod
    def setUpClass(cls) -> None:
        raw_dir = Path(
            os.environ.get("OLIST_TEST_RAW_DIR", REPOSITORY_ROOT / "data/raw")
        ).resolve()
        manifest_path = Path(
            os.environ.get(
                "OLIST_TEST_MANIFEST",
                REPOSITORY_ROOT / "config/raw_dataset_manifest.json",
            )
        ).resolve()
        contract_path = Path(
            os.environ.get(
                "OLIST_TEST_CONTRACT", REPOSITORY_ROOT / "config/source_contract.json"
            )
        ).resolve()
        compose_file = Path(
            os.environ.get(
                "OLIST_TEST_COMPOSE_FILE",
                REPOSITORY_ROOT / "infrastructure/docker-compose.yml",
            )
        ).resolve()
        manifest = load_manifest(manifest_path)
        cls.manifest_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        errors = []
        if cls.requires_raw_snapshot:
            _, errors = verify_snapshot(manifest, raw_dir)
        cls.contract = load_contract(contract_path)
        errors.extend(validate_contract(cls.contract, manifest))
        if errors:
            raise RuntimeError("Source test preflight failed: " + "; ".join(errors))

        configuration = json.loads(
            require_success(
                run(
                    "docker",
                    "compose",
                    "-f",
                    str(compose_file),
                    "config",
                    "--format",
                    "json",
                )
            )
        )
        image = configuration["services"]["postgres"]["image"]
        cls.container = f"olist-source-test-{uuid4().hex[:12]}"
        # Cleanup is restricted to this suite's unique container and anonymous volume.
        cls.addClassCleanup(cls.remove_container)
        arguments = [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            cls.container,
            "--memory",
            "2g",
            "--shm-size",
            "256m",
            "--env",
            "POSTGRES_USER=olist_test_admin",
            "--env",
            "POSTGRES_PASSWORD=isolated_test_admin",
            "--env",
            "POSTGRES_DB=postgres",
            "--env",
            "TZ=UTC",
            "--mount",
            "type=volume,destination=/var/lib/postgresql/data",
            "--mount",
            f"type=bind,source={manifest_path},destination=/config/raw_dataset_manifest.json,readonly",
        ]
        if cls.publish_port:
            arguments.extend(["--publish", "127.0.0.1::5432"])
        else:
            arguments.extend(["--network", "none"])
        if cls.requires_raw_snapshot:
            arguments.extend(
                [
                    "--mount",
                    f"type=bind,source={raw_dir},destination=/data/raw,readonly",
                ]
            )
        for directory in ["scripts", "migrations", "seeds", "checks"]:
            source = REPOSITORY_ROOT / "infrastructure/postgres" / directory
            arguments.extend(
                [
                    "--mount",
                    f"type=bind,source={source},destination=/{directory},readonly",
                ]
            )
        require_success(run(*arguments, image))
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            result = run(
                "docker",
                "exec",
                cls.container,
                "pg_isready",
                "--host=127.0.0.1",
                "-U",
                "olist_test_admin",
                "-d",
                "postgres",
            )
            if result.returncode == 0:
                break
            time.sleep(0.5)
        else:
            logs = run("docker", "logs", cls.container)
            raise RuntimeError(f"Test PostgreSQL did not become ready:\n{logs.stderr}")
        if cls.publish_port:
            address = require_success(run("docker", "port", cls.container, "5432/tcp"))
            cls.port = int(address.rsplit(":", 1)[1])
        cls.catalog_sql = (
            REPOSITORY_ROOT / "infrastructure/postgres/checks/export_source_schema.sql"
        ).read_text(encoding="utf-8")

    @classmethod
    def remove_container(cls) -> None:
        result = run("docker", "rm", "--force", "--volumes", cls.container)
        if result.returncode and "No such container" not in result.stderr:
            raise RuntimeError(f"Cannot remove test container: {result.stderr}")

    def setUp(self) -> None:
        self.database = f"source_test_{uuid4().hex[:12]}"
        self.psql(f"CREATE DATABASE {identifier(self.database)};", database="postgres")
        self.addCleanup(self.drop_database)
        require_success(self.job("run_migrations.sh"))

    def drop_database(self) -> None:
        self.psql(
            f"DROP DATABASE {identifier(self.database)} WITH (FORCE);",
            database="postgres",
        )

    def psql(
        self, sql: str, *, user: str = "olist_test_admin", database: str | None = None
    ) -> str:
        return require_success(
            run(
                "docker",
                "exec",
                "--interactive",
                self.container,
                "psql",
                "--no-psqlrc",
                "--set=ON_ERROR_STOP=1",
                "--quiet",
                "--tuples-only",
                "--no-align",
                "--username",
                user,
                "--dbname",
                database or self.database,
                sql=sql,
            )
        )

    def job(self, script: str) -> subprocess.CompletedProcess:
        environment = {
            "PGHOST": "127.0.0.1",
            "PGPORT": "5432",
            "PGOPTIONS": "-c statement_timeout=60000",
            "POSTGRES_DB": self.database,
            "POSTGRES_USER": "olist_test_admin",
            "POSTGRES_PASSWORD": "isolated_test_admin",
            "SOURCE_LOADER_USER": "olist_test_loader",
            "SOURCE_LOADER_PASSWORD": "isolated_test_loader",
            "SOURCE_READER_USER": "olist_test_reader",
            "SOURCE_READER_PASSWORD": "isolated_test_reader",
            "SOURCE_SNAPSHOT_ID": "olist-kaggle-local-v1",
            "SOURCE_SEED_BATCH_ID": "seed_olist_kaggle_local_v1",
            "SOURCE_SEED_TIMESTAMP": "2020-01-01T00:00:00Z",
        }
        arguments = ["docker", "exec"]
        for key, value in environment.items():
            arguments.extend(["--env", f"{key}={value}"])
        return run(*arguments, self.container, "/bin/sh", f"/scripts/{script}")
