"""Normalize verified orders batches or customer snapshots into Silver Parquet."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError

from olist_platform.bronze.storage import BronzeError, ObjectStore, s3_client
from olist_platform.raw.contracts import ContractError, load_contract

from .batch import process_batch, verify_silver


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
    parser.add_argument("--table", choices=("orders", "customers"), default="orders")
    commands = parser.add_subparsers(dest="command", required=True)
    process = commands.add_parser("process")
    process.add_argument("--bronze-prefix", required=True)
    process.add_argument("--run-id", required=True)
    process.add_argument("--master", default="local[2]")
    process.add_argument(
        "--customer-snapshot",
        help="Verified full customer Silver prefix; enables orders FK routing",
    )
    verify = commands.add_parser("verify")
    verify.add_argument("--prefix", required=True)
    arguments = parser.parse_args()
    spark = None
    try:
        store = ObjectStore(s3_client(arguments.endpoint), arguments.bucket)
        if arguments.command == "verify":
            result = {
                "status": "VERIFIED",
                "manifest": verify_silver(store, arguments.prefix),
            }
        else:
            try:
                from .orders import spark_session
            except ModuleNotFoundError:
                print(
                    "Install processing dependencies with: uv sync --locked --extra processing",
                    file=sys.stderr,
                )
                return 1
            spark = spark_session(arguments.master)
            result = process_batch(
                spark,
                store,
                load_contract(arguments.contract),
                bronze_prefix=arguments.bronze_prefix,
                run_id=arguments.run_id,
                table=arguments.table,
                customer_snapshot=arguments.customer_snapshot,
            )
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except (BronzeError, ContractError) as error:
        print(f"Silver processing failed: {error}", file=sys.stderr)
        return 1
    except (BotoCoreError, ClientError, OSError, ValueError, KeyError) as error:
        print(
            f"Silver processing failed ({type(error).__name__}); check input integrity and S3 configuration",
            file=sys.stderr,
        )
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
