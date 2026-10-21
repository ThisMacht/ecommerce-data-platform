"""Extract core source tables into immutable Bronze."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import psycopg
from botocore.exceptions import BotoCoreError, ClientError

from olist_platform.raw.contracts import ContractError, load_contract

from .extraction import KEYS, ingest_source
from .storage import BronzeError, ObjectStore, s3_client, verify_published


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--contract", type=Path, default=Path("config/source_contract.json")
    )
    parser.add_argument(
        "--endpoint",
        default=os.environ.get("BRONZE_S3_ENDPOINT", "http://127.0.0.1:9000"),
    )
    parser.add_argument(
        "--bucket", default=os.environ.get("BRONZE_S3_BUCKET", "olist-bronze")
    )
    parser.add_argument("--table", choices=tuple(KEYS), default="orders")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="Create the bucket if absent")
    extract = commands.add_parser("extract", help="Prepare or resume a named run")
    extract.add_argument("--run-id", required=True)
    extract.add_argument("--page-size", type=int, default=1000)
    extract.add_argument(
        "--pipeline-run-id", help="Publish a previously frozen incremental stream"
    )
    extract.add_argument("--pipeline-parent", action="store_true")
    extract.add_argument(
        "--pipeline-mode", choices=("incremental", "backfill"), default="incremental"
    )
    extract.add_argument(
        "--full-snapshot",
        action="store_true",
        help="Full source snapshot for parent checks or pipeline gates",
    )
    verify = commands.add_parser(
        "verify", help="Verify a published manifest and all objects"
    )
    verify.add_argument("--prefix", required=True)
    arguments = parser.parse_args()
    try:
        store = ObjectStore(s3_client(arguments.endpoint), arguments.bucket)
        if arguments.command == "init":
            store.initialize()
            result = {"status": "READY", "bucket": arguments.bucket}
        elif arguments.command == "verify":
            result = {
                "status": "VERIFIED",
                "manifest": verify_published(store, arguments.prefix),
            }
        else:
            contract = load_contract(arguments.contract)
            with psycopg.connect(
                os.environ.get("BRONZE_SOURCE_DSN", ""),
                autocommit=True,
                connect_timeout=10,
                application_name=f"olist-bronze-{arguments.table}",
            ) as connection:
                result = ingest_source(
                    connection,
                    store,
                    contract,
                    run_id=arguments.run_id,
                    endpoint=arguments.endpoint,
                    page_size=arguments.page_size,
                    table=arguments.table,
                    full_snapshot=arguments.full_snapshot,
                    pipeline_run_id=arguments.pipeline_run_id,
                    pipeline_parent=arguments.pipeline_parent,
                    pipeline_mode=arguments.pipeline_mode,
                )
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except (
        BronzeError,
        ContractError,
        psycopg.Error,
        BotoCoreError,
        ClientError,
        OSError,
        ValueError,
        KeyError,
    ) as error:
        # Drivers can include connection URLs; avoid echoing credentials on failure.
        if isinstance(error, (BronzeError, ContractError)):
            print(f"Bronze extraction failed: {error}", file=sys.stderr)
        else:
            print(
                f"Bronze extraction failed ({type(error).__name__}); check connectivity, credentials and batch integrity",
                file=sys.stderr,
            )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
