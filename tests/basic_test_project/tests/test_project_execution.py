import os
import sys
import unittest

# Change to the test project directory
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
os.chdir(os.path.join(os.path.dirname(__file__), ".."))
import pipelines.data_generation
import pipelines.offsets_pipeline
import systems.gen_offset_sys


class TestPipelines(unittest.TestCase):
    """
    Test pipeline related functions
    """

    def test_mix_pipeline(self):
        """
        Test running a pipeline with no datahandler or specified Node output, and a datahandler pipeline with output to disk
        """
        # Run the data generation pipeline and offsets pipeline
        pipelines.data_generation.data_gen()
        pipelines.offsets_pipeline.offset_pipe()

        # Check if the data was generated
        self.assertTrue(os.path.isdir("data/raw_signals"), "Data was not generated")
        self.assertTrue(os.path.isdir("data/offset_signals"), "Offsets were not generated")
        self.assertTrue(os.path.isdir("data/substracted_signals"), "Substracted signals were not generated")
        self.assertTrue(os.path.isdir("data/split_signals1"), "Split signals 1 were not generated")
        self.assertTrue(os.path.isdir("data/split_signals2"), "Split signals 2 were not generated")

        # Assert that both raw_signals and offsets have the same number of files
        raw_signals = os.listdir("data/raw_signals")
        offset_signals = os.listdir("data/offset_signals")
        self.assertEqual(len(raw_signals), len(offset_signals), "Raw signals and offsets have different number of files")

        # Assert that both raw_signals and substracted_signals have the same number of files
        substracted_signals = os.listdir("data/substracted_signals")
        self.assertEqual(len(raw_signals), len(substracted_signals), "Raw signals and substracted signals have different number of files")

        # Assert that split_signals1 and split_signals2 have the same number of files
        split_signals1 = os.listdir("data/split_signals1")
        split_signals2 = os.listdir("data/split_signals2")
        self.assertEqual(len(split_signals1), len(split_signals2), "Split signals 1 and 2 have different number of files")

        # Assert that both split_signals1 and split_signals2 have the same number of files as raw_signals
        self.assertEqual(len(raw_signals), len(split_signals1), "Raw signals and split signals have different number of files")

        # Clean up
        os.system("rm -rf data/raw_signals")
        os.system("rm -rf data/offset_signals")
        os.system("rm -rf data/substracted_signals")
        os.system("rm -rf data/split_signals1")
        os.system("rm -rf data/split_signals2")

class TestSystems(unittest.TestCase):
    """
    Test pipeline system related functions
    """

    def test_pipeline_system(self):
        """
        Test running all the project defined pipelines in one system
        """
        # Run the data generation and offsets system
        systems.gen_offset_sys.gen_offset_sys()

        # Check if the data was generated
        self.assertTrue(os.path.isdir("data/raw_signals"), "Data was not generated")
        self.assertTrue(os.path.isdir("data/offset_signals"), "Offsets were not generated")
        self.assertTrue(os.path.isdir("data/substracted_signals"), "Substracted signals were not generated")
        self.assertTrue(os.path.isdir("data/split_signals1"), "Split signals 1 were not generated")
        self.assertTrue(os.path.isdir("data/split_signals2"), "Split signals 2 were not generated")

        # Assert that both raw_signals and offsets have the same number of files
        raw_signals = os.listdir("data/raw_signals")
        offset_signals = os.listdir("data/offset_signals")
        self.assertEqual(len(raw_signals), len(offset_signals), "Raw signals and offsets have different number of files")

        # Assert that both raw_signals and substracted_signals have the same number of files
        substracted_signals = os.listdir("data/substracted_signals")
        self.assertEqual(len(raw_signals), len(substracted_signals), "Raw signals and substracted signals have different number of files")

        # Assert that split_signals1 and split_signals2 have the same number of files
        split_signals1 = os.listdir("data/split_signals1")
        split_signals2 = os.listdir("data/split_signals2")
        self.assertEqual(len(split_signals1), len(split_signals2), "Split signals 1 and 2 have different number of files")

        # Assert that both split_signals1 and split_signals2 have the same number of files as raw_signals
        self.assertEqual(len(raw_signals), len(split_signals1), "Raw signals and split signals have different number of files")

        # Clean up
        os.system("rm -rf data/raw_signals")
        os.system("rm -rf data/offset_signals")
        os.system("rm -rf data/substracted_signals")
        os.system("rm -rf data/split_signals1")
        os.system("rm -rf data/split_signals2")


if __name__ == '__main__':
    unittest.main()
