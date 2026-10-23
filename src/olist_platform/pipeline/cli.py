"""Audit a local core run and build dbt before atomically committing watermarks."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import psycopg
from botocore.exceptions import BotoCoreError, ClientError

from olist_platform.bronze.storage import (
    BronzeError,
    ObjectStore,
    encode,
    s3_client,
    sha256,
)
from olist_platform.raw.contracts import ContractError, load_contract

from .control import (
    ANALYTICS_SOURCES,
    backfill_window,
    begin,
    code_hash,
    current,
    dbt_evidence,
    evidence,
    fail,
    lock,
    promote,
    task,
)


def configuration(root: Path, contract: dict) -> dict:
    return {
        "processor_version": "core-item-dependencies-v2",
        "mode": os.environ.get("DATASET_MODE", "full"),
        **(
            {
                "backfill_window": backfill_window(
                    os.environ.get("BACKFILL_START"), os.environ.get("BACKFILL_END")
                )
            }
            if os.environ.get("DATASET_MODE") == "backfill"
            else {}
        ),
        "code_sha256": code_hash(root),
        "contract_sha256": sha256(encode(contract)),
        "database": {
            name: os.environ[name]
            for name in ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER")
        },
        "endpoint": os.environ.get("BRONZE_S3_ENDPOINT", "http://127.0.0.1:9000"),
        "bucket": os.environ.get("BRONZE_S3_BUCKET", "olist-bronze"),
        "dbt_reader": os.environ["DBT_READER_USER"],
        "orchestration": {
            "dag_id": os.environ.get("OLIST_AIRFLOW_DAG_ID"),
            "run_id": os.environ.get("OLIST_AIRFLOW_RUN_ID"),
        },
    }


def finish(
    connection,
    store,
    contract: dict,
    root: Path,
    run_id: str,
    attempt: int,
    request: dict,
    target: Path,
) -> dict:
    previous = current(connection, run_id)
    if previous["request_sha256"] != sha256(encode(request)):
        raise BronzeError("Pipeline code/configuration changed")
    if previous["status"] == "SUCCESS":
        return previous["result"] | {"status": "NOOP"}
    task(connection, run_id, attempt, "dbt.build", "RUNNING")
    try:
        with connection.transaction():
            lock(connection)
            row = current(connection, run_id, attempt)
            for table in ANALYTICS_SOURCES:
                connection.execute(
                    psycopg.sql.SQL("LOCK TABLE {} IN SHARE MODE").format(
                        psycopg.sql.Identifier("raw_staging", table)
                    )
                )
            items = evidence(connection, store, contract, run_id)
            earliest = max(
                [row["started_at"]]
                + [
                    receipt["committed_at"]
                    for item in items.values()
                    for name, receipt in item.items()
                    if name in ("load", "registration", "resolution", "retry_load")
                ]
            )
            target.mkdir(parents=True, exist_ok=True)
            env = os.environ.copy()
            env["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
            command = [
                str(Path(sys.executable).parent / "dbt"),
                "build",
                "--project-dir",
                str(root / "dbt"),
                "--profiles-dir",
                str(root / "dbt"),
                "--target-path",
                str(target),
            ]
            subprocess.run(
                command,
                env=env,
                check=True,
                stdout=sys.stderr,
                stderr=sys.stderr,
                timeout=600,
            )
            proof = dbt_evidence(target, earliest, connection.info.dbname)
            if code_hash(root) != request["code_sha256"]:
                raise BronzeError("Pipeline source changed during dbt build")
            task(connection, run_id, attempt, "dbt.build", "SUCCESS")
            return promote(connection, run_id, attempt, items, proof)
    except BaseException:
        # Data-plane receipts remain durable; a failed gate never advances control.
        fail(connection, run_id, attempt, "dbt.build")
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--run-id", required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("begin")
    frozen = commands.add_parser("freeze")
    frozen.add_argument("--attempt", type=int, required=True)
    event = commands.add_parser("task")
    event.add_argument("--attempt", type=int, required=True)
    event.add_argument("--name", required=True)
    event.add_argument(
        "--status", choices=("RUNNING", "SUCCESS", "FAILED"), required=True
    )
    failure = commands.add_parser("fail")
    failure.add_argument("--attempt", type=int, required=True)
    failure.add_argument("--name", default="verification")
    complete = commands.add_parser("finish")
    complete.add_argument("--attempt", type=int, required=True)
    complete.add_argument("--dbt-target", type=Path, required=True)
    args = parser.parse_args()
    try:
        root = args.root.resolve()
        contract = load_contract(root / "config/source_contract.json")
        with psycopg.connect(
            "", autocommit=True, connect_timeout=10, application_name="olist-pipeline"
        ) as connection:
            if args.command == "begin":
                result = begin(connection, args.run_id, configuration(root, contract))
            elif args.command == "task":
                task(connection, args.run_id, args.attempt, args.name, args.status)
                result = {"status": "RECORDED"}
            elif args.command == "fail":
                fail(connection, args.run_id, args.attempt, args.name)
                result = {"status": "FAILED"}
            else:
                store = ObjectStore(
                    s3_client(
                        os.environ.get("BRONZE_S3_ENDPOINT", "http://127.0.0.1:9000")
                    ),
                    os.environ.get("BRONZE_S3_BUCKET", "olist-bronze"),
                )
                if args.command == "freeze":
                    from .incremental import freeze

                    task(
                        connection,
                        args.run_id,
                        args.attempt,
                        "source.freeze",
                        "RUNNING",
                    )
                    result = freeze(
                        connection,
                        store,
                        contract,
                        args.run_id,
                        args.attempt,
                        os.environ.get("BRONZE_S3_ENDPOINT", "http://127.0.0.1:9000"),
                    )
                    task(
                        connection,
                        args.run_id,
                        args.attempt,
                        "source.freeze",
                        "SUCCESS",
                    )
                else:
                    result = finish(
                        connection,
                        store,
                        contract,
                        root,
                        args.run_id,
                        args.attempt,
                        configuration(root, contract),
                        args.dbt_target.resolve(),
                    )
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except (BronzeError, ContractError) as error:
        print(f"Pipeline gate failed: {error}", file=sys.stderr)
        return 1
    except (
        psycopg.Error,
        BotoCoreError,
        ClientError,
        OSError,
        ValueError,
        KeyError,
        TypeError,
        subprocess.SubprocessError,
    ) as error:
        print(
            f"Pipeline gate failed ({type(error).__name__}); check receipts, configuration and dbt artifacts",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
