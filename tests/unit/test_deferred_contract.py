import unittest

from olist_platform.bronze.storage import BronzeError
from olist_platform.deferred.lifecycle import validate_policy


class DeferredContractTest(unittest.TestCase):
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
