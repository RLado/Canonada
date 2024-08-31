import os
import sys
import unittest

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
sys.argv = ["canonada", "new"]
import canonada.catalog._core as catalog


class TestCatalog(unittest.TestCase):
    """
    Test catalog functions
    """

    def test_flatten(self):
        """
        Test the flatten function
        """
        # Test a nested dictionary
        nested_dict = {
            "a": 1,
            "b": {
                "c": 2,
                "d": {
                    "e": 3,
                    "fg": "test.dot",
                    "jk": {
                        "l": 4,
                        "m": 5
                    }
                }
            }
        }
        flat_dict = catalog._flatten(nested_dict)
        expected_dict = {
            "a": 1,
            "b.c": 2,
            "b.d.e": 3,
            "b.d.fg": "test.dot",
            "b.d.jk.l": 4,
            "b.d.jk.m": 5
        }
        self.assertEqual(flat_dict, expected_dict)

if __name__ == '__main__':
    unittest.main()
