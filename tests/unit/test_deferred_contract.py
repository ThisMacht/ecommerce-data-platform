import unittest

from olist_platform.bronze.storage import BronzeError
from olist_platform.deferred.lifecycle import dependency_scope, validate_policy
from olist_platform.silver.batch import ITEM_SCOPE


class DeferredContractTest(unittest.TestCase):
    def test_item_scope_is_explicit_and_rejects_other_tables(self):
        self.assertTrue(
            dependency_scope("order_items", {"dependency_scope": ITEM_SCOPE})
        )
        self.assertFalse(dependency_scope("order_items", {}))
        for table, scope in (
            ("orders", ITEM_SCOPE),
            ("order_payments", ITEM_SCOPE),
            ("order_items", "unknown"),
            ("unknown", None),
        ):
            with self.subTest(table=table, scope=scope), self.assertRaises(BronzeError):
                dependency_scope(table, {"dependency_scope": scope})

    def test_limits_are_positive_bounded_integers(self):
        validate_policy(3, 168)
        for attempts, hours in (
            (True, 168),
            (0, 168),
            (101, 168),
            (3, False),
            (3, 0),
            (3, 8761),
            (1.5, 168),
        ):
            with (
                self.subTest(attempts=attempts, hours=hours),
                self.assertRaises(BronzeError),
            ):
                validate_policy(attempts, hours)
