"""Independent dirty input values and expected outcomes shared by Spark tests."""

import json
from pathlib import Path

from olist_platform.bronze.storage import encode, sha256

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests/fixtures/silver"


def customer_rows() -> list[dict]:
    result = []
    base = {
        "customer_unique_id": "shared_shopper",
        "customer_zip_code_prefix": "00123",
        "customer_city": " São Paulo ",
        "customer_state": "sp",
        "source_updated_at": "2020-01-01T00:00:00+00:00",
        "source_loaded_at": "2020-01-01T00:00:00+00:00",
        "source_operation": "INSERT",
        "source_batch_id": "fixture",
    }
    for number, value in enumerate(
        json.loads((FIXTURES / "customers_dirty.json").read_text()), 1
    ):
        row = base | value
        row.update(_record_uid=f"uid-{number}", _raw_payload=encode(row).decode())
        result.append(row)
    return result


def child_rows(table: str) -> list[dict]:
    base = {
        "source_updated_at": "2020-01-01T00:00:00+00:00",
        "source_loaded_at": "2020-01-01T00:00:00+00:00",
        "source_operation": "INSERT",
        "source_batch_id": "child_fixture",
    }
    base.update(
        {
            "product_id": " product ",
            "seller_id": " seller ",
            "shipping_limit_date": "2018-01-01T12:00:00",
            "price": "10.50",
            "freight_value": "1.25",
        }
        if table == "order_items"
        else {
            "payment_type": " CREDIT_CARD ",
            "payment_installments": 1,
            "payment_value": "10.50",
        }
    )
    result = []
    for number, value in enumerate(
        json.loads((FIXTURES / f"{table}_dirty.json").read_text()), 1
    ):
        row = base | value
        row.update(_record_uid=f"child-{number:02}", _raw_payload=encode(row).decode())
        result.append(row)
    return result


def rows() -> list[dict]:
    base = {
        "customer_id": "customer_a",
        "order_status": "approved",
        "order_purchase_timestamp": "2018-01-01T12:00:00",
        "order_approved_at": None,
        "order_delivered_carrier_date": None,
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": "2018-01-10T00:00:00",
        "source_updated_at": "2020-01-04T00:00:00+00:00",
        "source_operation": "INSERT",
        "source_batch_id": "dirty_fixture",
        "source_loaded_at": "2020-01-04T00:00:00+00:00",
    }
    values = json.loads((FIXTURES / "orders_dirty.json").read_text())
    result = []
    for number, value in enumerate(values, 1):
        row = base | value
        row.update(
            _record_uid=f"uid-{number:02}",
            _logical_record_uid=None,
            _ingested_at="2026-10-21T00:00:00+00:00",
            _run_id="dirty",
            _run_attempt=1,
            _source_name="postgresql.orders",
            _source_file="fixture.jsonl",
            _source_row_number=number,
            _schema_version="1.0.0",
            _raw_payload=encode(row).decode().strip(),
        )
        result.append(row)
    return result


def dirty_rows(table: str) -> list[dict]:
    """Keep legacy cases and add a missing-parent order for manifest verification."""
    if table == "customers":
        return customer_rows()
    if table in ("order_items", "order_payments"):
        return child_rows(table)
    if table != "orders":
        raise ValueError(f"Unsupported dirty fixture table: {table}")
    values = rows()
    for row in values:
        if row["order_id"] == "winter":
            row["customer_id"] = "late_customer"
            payload = json.loads(row["_raw_payload"])
            payload["customer_id"] = "late_customer"
            row["_raw_payload"] = encode(payload).decode().strip()
    return values


def publish_bronze(
    store, contract: dict, prefix="bronze/orders/ingestion_date=2026-10-21/run_id=dirty"
) -> str:
    from olist_platform.silver.batch import LINEAGE

    values = rows()
    key = f"{prefix}/attempt=0001/part-000001.jsonl"
    source_schema = (
        next(t["columns"] for t in contract["tables"] if t["table"] == "orders")
        + contract["metadata_columns"]
    )
    for row in values:
        row.pop("_raw_payload")
        row["_source_file"] = key
        row["_record_uid"] = sha256(encode(["dirty", key, row["_source_row_number"]]))
        row["_logical_record_uid"] = (
            sha256(
                encode(["postgresql.orders", row["order_id"], row["source_sequence"]])
            )
            if row["order_id"]
            else None
        )
        assert set(row) == {column["name"] for column in source_schema} | set(LINEAGE)
    body = b"".join(encode(row) for row in values)
    manifest = {
        "format_version": 1,
        "schema_version": "1.0.0",
        "source": "postgresql.orders",
        "run_id": "dirty",
        "prefix": prefix,
        "prepared_at": "2026-10-21T00:00:00+00:00",
        "config": {"contract_sha256": sha256(encode(contract))},
        "source_schema": source_schema,
        "schema_fingerprint": sha256(encode(source_schema)),
        "row_count": len(values),
        "source_row_count": len(values),
        "page_count": 1,
        "objects": [
            {
                "key": key,
                "sha256": sha256(body),
                "size_bytes": len(body),
                "row_count": len(values),
            }
        ],
    }
    store.put_immutable(key, body)
    store.put_immutable(f"{prefix}/manifest.json", encode(manifest))
    store.put_immutable(
        f"{prefix}/_SUCCESS", encode({"manifest_sha256": sha256(encode(manifest))})
    )
    return prefix
