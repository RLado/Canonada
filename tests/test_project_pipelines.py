import os
import sys
import unittest

# Import pipelines
sys.path.append(os.path.join(os.path.dirname(__file__), "basic_test_project"))
import pipelines.data_generation

class TestPipelines(unittest.TestCase):
    """
    Test pipeline related functions
    """

    def test_no_datahandler_pipeline(self):
        """
        Test running a pipeline with no datahandler or specified Node output
        """
        # Run the data generation pipeline
        pipelines.data_generation.data_gen()

        # Check if the data was generated
        self.assertTrue(os.path.isdir("data/raw_signals"))


if __name__ == '__main__':
    unittest.main()
