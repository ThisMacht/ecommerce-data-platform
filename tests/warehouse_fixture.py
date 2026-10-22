"""Independent typed valid rows for hashing and PostgreSQL merge checks."""

from datetime import datetime, timezone
from decimal import Decimal


def valid_row(table="customers", key="a", sequence=1):
    instant = datetime(2020, 1, 1, tzinfo=timezone.utc)
    row = {
        "sellers": {
            "seller_id": key,
            "seller_zip_code_prefix": "00123",
            "seller_city": "são paulo",
            "seller_state": "SP",
        },
        "products": {
            "product_id": key,
            "product_category_name": "cama_mesa_banho",
            "product_name_lenght": 20,
            "product_description_lenght": 100,
            "product_photos_qty": 1,
            "product_weight_g": 200,
            "product_length_cm": 10,
            "product_height_cm": 5,
            "product_width_cm": 8,
        },
        "customers": {
            "customer_id": key,
            "customer_unique_id": "shopper",
            "customer_city": "são paulo",
            "customer_state": "SP",
            "customer_zip_code_prefix": "00123",
        },
        "orders": {
            "order_id": key,
            "customer_id": "parent",
            "order_status": "approved",
            "order_purchase_timestamp": instant,
            "order_approved_at": None,
            "order_delivered_carrier_date": None,
            "order_delivered_customer_date": None,
            "order_estimated_delivery_date": datetime(2020, 1, 10, tzinfo=timezone.utc),
            "order_purchase_date": "2019-12-31",
        },
        "order_items": {
            "order_id": key,
            "order_item_id": 1,
            "product_id": "product",
            "seller_id": "seller",
            "shipping_limit_date": instant,
            "price": Decimal("10.50"),
            "freight_value": Decimal("1.25"),
        },
        "order_payments": {
            "order_id": key,
            "payment_sequential": 1,
            "payment_type": "credit_card",
            "payment_installments": 1,
            "payment_value": Decimal("10.50"),
        },
    }[table]
    row.update(
        source_updated_at=instant,
        source_loaded_at=instant,
        source_sequence=sequence,
        source_operation="INSERT",
        source_batch_id="fixture",
        _record_uid=f"{sequence:064x}",
        _logical_record_uid="a" * 64,
        _ingested_at=instant,
        _run_id="silver_fixture",
        _run_attempt=1,
        _source_name=f"postgresql.{table}",
        _source_file="fixture.jsonl",
        _source_row_number=sequence,
        _schema_version="1.0.0",
        error_rule_ids=[],
        warning_rule_ids=[],
        deferred_rule_ids=[],
    )
    return row
