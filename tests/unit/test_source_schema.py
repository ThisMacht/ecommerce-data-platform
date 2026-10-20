from __future__ import annotations

import unittest
from copy import deepcopy

from olist_platform.raw.schema import validate_database_schema


class SourceSchemaTest(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = {
            "database": {"source_schema": "olist"},
            "policies": {"schema_evolution": {"additive_nullable_column": "WARN"}},
            "tables": [
                {
                    "table": "items",
                    "primary_key": ["order_id", "item_id"],
                    "columns": [
                        {
                            "name": "order_id",
                            "postgres_type": "text",
                            "nullable": False,
                        },
                        {
                            "name": "item_id",
                            "postgres_type": "integer",
                            "nullable": False,
                        },
                        {
                            "name": "price",
                            "postgres_type": "numeric(18,2)",
                            "nullable": False,
                        },
                    ],
                }
            ],
            "metadata_columns": [
                {
                    "name": "source_sequence",
                    "postgres_type": "bigint",
                    "nullable": False,
                }
            ],
        }
        self.catalog = {
            "schemas": {
                "olist": {
                    "items": {
                        "columns": [
                            {
                                "name": "order_id",
                                "postgres_type": "text",
                                "nullable": False,
                                "identity": "",
                            },
                            {
                                "name": "item_id",
                                "postgres_type": "integer",
                                "nullable": False,
                                "identity": "",
                            },
                            {
                                "name": "price",
                                "postgres_type": "numeric(18, 2)",
                                "nullable": False,
                                "identity": "",
                            },
                            {
                                "name": "source_sequence",
                                "postgres_type": "bigint",
                                "nullable": False,
                                "identity": "d",
                            },
                        ],
                        "primary_key": ["order_id", "item_id"],
                        "unique_keys": [["order_id", "item_id"], ["source_sequence"]],
                    }
                }
            }
        }

    def check_catalog(self, catalog: dict) -> tuple[list[str], list[str]]:
        return validate_database_schema(self.contract, catalog)

    def test_matching_catalog_accepts_numeric_formatting(self) -> None:
        self.assertEqual(self.check_catalog(self.catalog), ([], []))

    def test_type_precision_drift_is_blocking(self) -> None:
        self.catalog["schemas"]["olist"]["items"]["columns"][2]["postgres_type"] = (
            "numeric(20,4)"
        )
        errors, _ = self.check_catalog(self.catalog)
        self.assertTrue(any("Type mismatch for olist.items.price" in e for e in errors))

    def test_nullability_drift_is_blocking(self) -> None:
        self.catalog["schemas"]["olist"]["items"]["columns"][2]["nullable"] = True
        errors, _ = self.check_catalog(self.catalog)
        self.assertTrue(any("Nullability mismatch" in e for e in errors))

    def test_composite_primary_key_order_is_checked(self) -> None:
        self.catalog["schemas"]["olist"]["items"]["primary_key"] = [
            "item_id",
            "order_id",
        ]
        errors, _ = self.check_catalog(self.catalog)
        self.assertTrue(any("Primary-key mismatch" in e for e in errors))

    def test_missing_business_or_metadata_column_is_blocking(self) -> None:
        for name in ["order_id", "source_sequence"]:
            with self.subTest(column=name):
                catalog = deepcopy(self.catalog)
                columns = catalog["schemas"]["olist"]["items"]["columns"]
                columns[:] = [column for column in columns if column["name"] != name]
                errors, _ = self.check_catalog(catalog)
                self.assertIn(f"Missing column: olist.items.{name}", errors)

    def test_missing_schema_or_table_is_blocking(self) -> None:
        self.assertEqual(
            self.check_catalog({"schemas": {}})[0], ["Missing source schema: olist"]
        )
        self.assertEqual(
            self.check_catalog({"schemas": {"olist": {}}})[0],
            ["Missing source table: olist.items"],
        )

    def test_nullable_addition_warns_and_required_addition_fails(self) -> None:
        columns = self.catalog["schemas"]["olist"]["items"]["columns"]
        columns.append({"name": "extra", "postgres_type": "text", "nullable": True})
        self.assertEqual(
            self.check_catalog(self.catalog),
            ([], ["Unexpected column: olist.items.extra"]),
        )
        columns[-1]["nullable"] = False
        self.assertEqual(
            self.check_catalog(self.catalog),
            (["Unexpected column: olist.items.extra"], []),
        )

    def test_sequence_requires_identity_and_single_column_uniqueness(self) -> None:
        table = self.catalog["schemas"]["olist"]["items"]
        table["columns"][-1]["identity"] = ""
        table["unique_keys"] = [["order_id", "item_id", "source_sequence"]]
        errors, _ = self.check_catalog(self.catalog)
        self.assertEqual(len(errors), 2)
        self.assertTrue(any("BY DEFAULT identity" in e for e in errors))
        self.assertTrue(any("unique constraint" in e for e in errors))

    def test_technical_primary_key_is_non_nullable_and_type_checked(self) -> None:
        self.contract["tables"][0]["technical_key"] = {
            "name": "row_id",
            "postgres_type": "bigint",
        }
        self.contract["tables"][0]["primary_key"] = ["row_id"]
        table = self.catalog["schemas"]["olist"]["items"]
        table["primary_key"] = ["row_id"]
        table["columns"].append(
            {"name": "row_id", "postgres_type": "integer", "nullable": True}
        )
        errors, _ = self.check_catalog(self.catalog)
        self.assertEqual(len(errors), 2)


if __name__ == "__main__":
    unittest.main()
