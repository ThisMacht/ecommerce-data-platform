"""Persist deferred Silver rows and resolve them against pinned full snapshots."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import psycopg
from botocore.exceptions import BotoCoreError, ClientError

from olist_platform.bronze.storage import BronzeError, ObjectStore, s3_client
from olist_platform.raw.contracts import ContractError, load_contract

from .lifecycle import DEPENDENCIES, register_silver, resolve_snapshot


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
    parser.add_argument("--table", choices=tuple(DEPENDENCIES), default="orders")
    parser.add_argument("--master", default="local[2]")
    commands = parser.add_subparsers(dest="command", required=True)
    register = commands.add_parser("register")
    register.add_argument("--silver-prefix", required=True)
    register.add_argument("--run-id", required=True)
    register.add_argument("--max-attempts", type=int, default=3)
    register.add_argument("--max-deferred-age-hours", type=int, default=168)
    resolve = commands.add_parser("resolve")
    resolve.add_argument("--parent-snapshot", required=True)
    resolve.add_argument("--run-id", required=True)
    resolve.add_argument(
        "--origin-run-id", help="Restrict pending rows to one original Silver run"
    )
    resolve.add_argument(
        "--product-snapshot",
        help="Full products Silver prefix; pair with --seller-snapshot for item FK checks",
    )
    resolve.add_argument(
        "--seller-snapshot",
        help="Full sellers Silver prefix; pair with --product-snapshot",
    )
    args = parser.parse_args()
    spark = None
    try:
        from olist_platform.silver.orders import spark_session

        contract = load_contract(args.contract)
        spark = spark_session(args.master)
        store = ObjectStore(s3_client(args.endpoint), args.bucket)
        with psycopg.connect(
            os.environ.get("WAREHOUSE_DSN", ""),
            autocommit=True,
            connect_timeout=10,
            application_name="olist-deferred",
        ) as connection:
            options = {"table": args.table, "run_id": args.run_id}
            if args.command == "register":
                result = register_silver(
                    spark,
                    connection,
                    store,
                    contract,
                    **options,
                    silver_prefix=args.silver_prefix,
                    max_attempts=args.max_attempts,
                    max_deferred_age_hours=args.max_deferred_age_hours,
                )
            else:
                result = resolve_snapshot(
                    spark,
                    connection,
                    store,
                    contract,
                    **options,
                    parent_snapshot=args.parent_snapshot,
                    origin_run_id=args.origin_run_id,
                    product_snapshot=args.product_snapshot,
                    seller_snapshot=args.seller_snapshot,
                )
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except (BronzeError, ContractError) as error:
        print(f"Deferred operation failed: {error}", file=sys.stderr)
        return 1
    except ModuleNotFoundError:
        print(
            "Install processing dependencies with: make sync-processing",
            file=sys.stderr,
        )
        return 1
    except (
        psycopg.Error,
        BotoCoreError,
        ClientError,
        OSError,
        ValueError,
        KeyError,
        TypeError,
    ) as error:
        print(
            f"Deferred operation failed ({type(error).__name__}); check input integrity and database/S3 configuration",
            file=sys.stderr,
        )
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
