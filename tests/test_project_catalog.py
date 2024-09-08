import os
import sys
import unittest

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
import canonada.catalog._core as catalog


class TestCatalog(unittest.TestCase):
    """
    Test catalog functions
    """

    def setUp(self):
        # Save the current directory
        self.original_directory = os.getcwd()
        # Change to the test project directory
        os.chdir("tests/basic_test_project")

    def tearDown(self):
        # Change back to the original directory
        os.chdir(self.original_directory)

    def test_flatten(self):
        """
        Test the flatten function
        """
        # Test a nested dictionary
        nested_dict = {
            "a": 1,
            "b": {"c": 2, "d": {"e": 3, "fg": "test.dot", "jk": {"l": 4, "m": 5}}},
        }
        flat_dict = catalog._flatten(nested_dict)
        expected_dict = {
            "a": 1,
            "b.c": 2,
            "b.d.e": 3,
            "b.d.fg": "test.dot",
            "b.d.jk.l": 4,
            "b.d.jk.m": 5,
        }
        self.assertEqual(flat_dict, expected_dict)

    def test_params(self):
        """
        Test the params function
        """

        # Test the params function
        params = catalog.params()

        # Verify results
        expected_params = {"sig_gen.num_signals": 200, "sig_gen.num_samples": 1000, "offset_signal.random_seed": 42}
        self.assertEqual(params, expected_params)

    def test_credentials(self):
        """
        Test the credentials function
        """

        # Test the credentials function
        credentials = catalog.credentials()

        # Verify results
        expected_credentials = {
            "service_name.user": "9jdsh3bsdus39sdhd3",
            "service_name.token": "8943bdhis79wygfw8924ysdjsdis9",
            "service_name_2.user": "u83gsdk83fsh3vzna<z93",
            "service_name_2.password": "8d3h983jbn34rfsujonusd8123joqew",
        }
        self.assertEqual(credentials, expected_credentials)

    def test_catalog(self):
        """
        Test the catalog list function
        """

        # Test the catalog function
        datasets = catalog.ls()

        # Verify results
        expected_datasets = ["raw_signals", "offset_signals", "substracted_signals", "split_signals1", "split_signals2"]
        self.assertEqual(datasets, expected_datasets)


if __name__ == "__main__":
    unittest.main()
