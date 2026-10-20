"""A small reliability fixture referencing the canonical Olist product/seller."""

from __future__ import annotations

import hashlib
from pathlib import Path

from .batches import BatchError, canonical_bytes, contract_hash, write_batch

# References from the first canonical order-item row; business attributes below
# are synthetic. The local source should be seeded before applying this fixture.
REFERENCE_PRODUCT_ID = "4244733e06e7ecb4970a6e2683c13e61"
REFERENCE_SELLER_ID = "48436dade18ac8b2bce089ec2a041202"


def fixture_ids(seed: int = 42) -> dict[str, str]:
    if type(seed) is not int or not 0 <= seed < 2**32:
        raise BatchError("Fixture seed must be a 32-bit unsigned integer")
    return {
        name: hashlib.sha256(f"olist-simulator-v1:{seed}:{name}".encode()).hexdigest()[
            :32
        ]
        for name in [
            "customer_1",
            "customer_2",
            "customer_3",
            "shopper_1",
            "shopper_2",
            "order_1",
            "order_2",
            "order_3",
        ]
    }


def fixture_batches(seed: int = 42) -> list[tuple[str, dict, dict]]:
    ids = fixture_ids(seed)

    def operation(
        table: str, action: str, key: dict, values: dict, timestamp: str
    ) -> dict:
        return {
            "table": table,
            "operation": action,
            "key": key,
            "values": values,
            "source_updated_at": timestamp,
        }

    def customer(number: int, timestamp: str) -> dict:
        return operation(
            "customers",
            "INSERT",
            {"customer_id": ids[f"customer_{number}"]},
            {
                "customer_unique_id": ids["shopper_1" if number < 3 else "shopper_2"],
                "customer_zip_code_prefix": "01000",
                "customer_city": "fixture city",
                "customer_state": "SP",
            },
            timestamp,
        )

    def order(number: int, timestamp: str) -> dict:
        return operation(
            "orders",
            "INSERT",
            {"order_id": ids[f"order_{number}"]},
            {
                "customer_id": ids[f"customer_{number}"],
                "order_status": "shipped" if number == 1 else "processing",
                "order_purchase_timestamp": "2018-01-01T12:00:00",
                "order_approved_at": "2018-01-01T13:00:00",
                "order_delivered_carrier_date": "2018-01-02T10:00:00"
                if number == 1
                else None,
                "order_delivered_customer_date": None,
                "order_estimated_delivery_date": "2018-01-10T00:00:00",
            },
            timestamp,
        )

    def item(number: int, timestamp: str) -> dict:
        return operation(
            "order_items",
            "INSERT",
            {"order_id": ids[f"order_{number}"], "order_item_id": 1},
            {
                "product_id": REFERENCE_PRODUCT_ID,
                "seller_id": REFERENCE_SELLER_ID,
                "shipping_limit_date": "2018-01-05T12:00:00",
                "price": "10.50",
                "freight_value": "1.25",
            },
            timestamp,
        )

    def payment(number: int, timestamp: str) -> dict:
        return operation(
            "order_payments",
            "INSERT",
            {"order_id": ids[f"order_{number}"], "payment_sequential": 1},
            {
                "payment_type": "credit_card",
                "payment_installments": 1,
                "payment_value": "11.75",
            },
            timestamp,
        )

    initial_time = "2020-01-02T00:00:00Z"
    child_time = "2020-01-03T00:00:00Z"
    parent_time = "2020-01-04T00:00:00Z"
    definitions = [
        (
            "001_initial",
            initial_time,
            [
                customer(1, initial_time),
                customer(2, initial_time),
                order(1, initial_time),
                order(2, initial_time),
                item(1, initial_time),
                item(2, initial_time),
                payment(1, initial_time),
            ],
            {
                "scenario": "same_timestamp",
                "fixture_rows_after": {
                    "customers": 2,
                    "orders": 2,
                    "order_items": 2,
                    "order_payments": 1,
                },
                "unresolved_order_ids": [],
            },
        ),
        (
            "002_children",
            child_time,
            [item(3, child_time), payment(3, child_time)],
            {
                "scenario": "children_before_order",
                "fixture_rows_after": {
                    "customers": 2,
                    "orders": 2,
                    "order_items": 3,
                    "order_payments": 2,
                },
                "unresolved_order_ids": [ids["order_3"]],
            },
        ),
        (
            "003_parents_updates",
            parent_time,
            [
                customer(3, parent_time),
                order(3, parent_time),
                operation(
                    "orders",
                    "UPDATE",
                    {"order_id": ids["order_1"]},
                    {
                        "order_status": "delivered",
                        "order_delivered_customer_date": "2018-01-04T13:00:00",
                    },
                    parent_time,
                ),
                operation(
                    "customers",
                    "UPDATE",
                    {"customer_id": ids["customer_1"]},
                    {"customer_city": "fixture updated"},
                    parent_time,
                ),
                payment(2, parent_time),
            ],
            {
                "scenario": "parents_resolve_and_late_payment",
                "fixture_rows_after": {
                    "customers": 3,
                    "orders": 3,
                    "order_items": 3,
                    "order_payments": 3,
                },
                "unresolved_order_ids": [],
            },
        ),
        (
            "004_backdated",
            "2020-01-05T00:00:00Z",
            [
                operation(
                    "orders",
                    "UPDATE",
                    {"order_id": ids["order_1"]},
                    {"order_status": "delivered"},
                    "2020-01-03T12:00:00Z",
                ),
                operation(
                    "customers",
                    "UPDATE",
                    {"customer_id": ids["customer_1"]},
                    {"customer_city": "fixture backdated correction"},
                    "2020-01-01T00:00:00Z",
                ),
            ],
            {
                "scenario": "newer_versions_with_older_timestamps",
                "fixture_rows_after": {
                    "customers": 3,
                    "orders": 3,
                    "order_items": 3,
                    "order_payments": 3,
                },
                "unresolved_order_ids": [],
                "lookback_reference": parent_time,
                "lookback_hours": 48,
                "within_lookback_order_ids": [ids["order_1"]],
                "outside_lookback_customer_ids": [ids["customer_1"]],
            },
        ),
    ]
    result = []
    predecessor = None
    for name, applied_at, operations, expected in definitions:
        batch_id = f"sim_v1_s{seed}_{name}"
        payload = {
            "format_version": 1,
            "batch_id": batch_id,
            "predecessor_batch_id": predecessor,
            "applied_at": applied_at,
            "operations": operations,
        }
        result.append((name, payload, expected))
        predecessor = batch_id
    return result


def write_fixture(directory: Path, contract: dict, seed: int = 42) -> list[Path]:
    batches = fixture_batches(seed)
    for name, payload, expected in batches:
        write_batch(directory / name, payload, contract, expected)
    index = canonical_bytes(
        {
            "fixture_version": 1,
            "seed": seed,
            "contract_sha256": contract_hash(contract),
            "batches": [
                {"directory": name, "batch_id": payload["batch_id"]}
                for name, payload, _ in batches
            ],
        }
    )
    path = directory / "fixture.json"
    if path.exists():
        if path.read_bytes() != index:
            raise BatchError("Refusing to overwrite a different fixture index")
    else:
        with path.open("xb") as handle:
            handle.write(index)
    return [directory / name for name, _, _ in batches]
