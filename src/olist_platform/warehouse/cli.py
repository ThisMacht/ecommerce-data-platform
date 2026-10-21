"""Load verified Silver valid rows into version-aware PostgreSQL staging."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import psycopg
from botocore.exceptions import BotoCoreError, ClientError

from olist_platform.bronze.extraction import KEYS
from olist_platform.bronze.storage import BronzeError, ObjectStore, s3_client
from olist_platform.raw.contracts import ContractError, load_contract

from .loading import load_silver


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
    parser.add_argument("--silver-prefix", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--master", default="local[2]")
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
            application_name="olist-warehouse",
        ) as connection:
            result = load_silver(
                spark,
                connection,
                store,
                contract,
                silver_prefix=args.silver_prefix,
                run_id=args.run_id,
                table=args.table,
            )
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except (BronzeError, ContractError) as error:
        print(f"Warehouse load failed: {error}", file=sys.stderr)
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
            f"Warehouse load failed ({type(error).__name__}); check input integrity and database/S3 configuration",
            file=sys.stderr,
        )
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
