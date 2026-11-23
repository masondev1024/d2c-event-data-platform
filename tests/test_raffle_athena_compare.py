import unittest

from scripts.raffle_athena_cloud import _compare, _oracle_index


class AthenaOracleTests(unittest.TestCase):
    def test_equal_totals_still_detect_wrong_product_and_unconfirmed_zero(self):
        oracle = [
            {"window_key": w, "product_id": p, "window_state": state, "entry_count": n}
            for w, p, state, n in [
                ("A", 1, "CLOSED", 3), ("A", 2, "CLOSED", 0),
                ("B", 1, "OPEN", None), ("B", 2, "OPEN", None),
            ]
        ]
        actual = [
            ("A", 1, "CLOSED", 2, 1), ("A", 2, "CLOSED", 1, 1),
            ("B", 1, "CLOSED", 0, 1), ("B", 2, "OPEN", None, 1),
        ]
        result = _compare(_oracle_index(oracle), actual)
        self.assertFalse(result["passed"])
        self.assertEqual(3, result["count_mismatches"])
        self.assertEqual(1, result["state_mismatches"])

    def test_row_count_alone_cannot_mask_missing_or_duplicate_keys(self):
        expected = {(w, p): ("CLOSED", 0) for w in "AB" for p in (1, 2)}
        result = _compare(expected, [
            ("A", 1, "CLOSED", 0, 2), ("B", 1, "CLOSED", 0, 1), ("B", 3, "CLOSED", 0, 1),
        ])
        self.assertEqual(4, result["actual_rows"])
        self.assertEqual(2, result["missing_keys"])
        self.assertEqual(1, result["extra_keys"])
        self.assertEqual(1, result["duplicate_rows"])
        self.assertFalse(result["passed"])
