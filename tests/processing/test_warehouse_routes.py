import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from olist_platform.bronze.storage import sha256
from olist_platform.silver.orders import spark_session
from olist_platform.warehouse.loading import read_route


class WarehouseRouteTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)

    def test_large_single_file_is_handed_off_in_small_partitions_without_row_loss(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            frame = self.spark.range(25001).coalesce(1)
            frame.write.parquet(str(root / "input"))
            frame = self.spark.read.parquet(str(root / "input"))
            body = next((root / "input").glob("*.parquet")).read_bytes()
            store = SimpleNamespace(read=lambda key: body)
            manifest = dict(
                output_schema=frame.schema.jsonValue(),
                counts={"valid_rows": 25001},
                objects=[dict(key="part.parquet", route="valid", sha256=sha256(body))],
            )
            batch_sizes = []
            original = type(frame).toLocalIterator

            def iterate(dataframe):
                batch_sizes.extend(
                    dataframe.rdd.mapPartitions(
                        lambda rows: [sum(1 for _ in rows)]
                    ).collect()
                )
                return original(dataframe)

            with patch.object(type(frame), "toLocalIterator", iterate):
                ids = [
                    row["id"]
                    for row in read_route(self.spark, store, manifest, root, "valid")
                ]
            self.assertGreater(len(batch_sizes), 1)
            self.assertLessEqual(max(batch_sizes), 10000)
            self.assertEqual(sorted(ids), list(range(25001)))
