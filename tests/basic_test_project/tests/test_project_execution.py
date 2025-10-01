import os
import sys
import unittest

# Change to the test project directory
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
os.chdir(os.path.join(os.path.dirname(__file__), ".."))
import pipelines.data_generation
import pipelines.offsets_pipeline
import systems.gen_offset_sys

import canonada.exceptions


class TestPipelines(unittest.TestCase):
    """
    Test pipeline related functions
    """

    def test_mix_pipeline_multiprocessing(self):
        """
        Test running a pipeline with no datahandler or specified Node output, and a datahandler pipeline with output to disk. (Using multiprocessing)
        """
        # Run the data generation pipeline and offsets pipeline
        data_gen_pipeline = pipelines.data_generation.data_gen
        data_gen_pipeline.multiprocessing = True
        offset_pipeline = pipelines.offsets_pipeline.offset_pipe
        offset_pipeline.multiprocessing = True
        
        data_gen_pipeline.run()
        offset_pipeline.run()

        # Check if the data was generated
        self.assertTrue(os.path.isdir("data/raw_signals"), "Data was not generated")
        self.assertTrue(os.path.isdir("data/offset_signals"), "Offsets were not generated")
        self.assertTrue(os.path.isdir("data/substracted_signals"), "Substracted signals were not generated")
        self.assertTrue(os.path.isdir("data/split_signals1"), "Split signals 1 were not generated")
        self.assertTrue(os.path.isdir("data/split_signals2"), "Split signals 2 were not generated")

        # Assert that both raw_signals and offsets have the same number of files
        raw_signals = os.listdir("data/raw_signals")
        offset_signals = os.listdir("data/offset_signals")
        self.assertEqual(len(raw_signals), len(offset_signals), "Raw signals and offsets have different a number of files")

        # Assert that both raw_signals and substracted_signals have the same number of files
        substracted_signals = os.listdir("data/substracted_signals")
        self.assertEqual(len(raw_signals), len(substracted_signals), "Raw signals and substracted signals have a different number of files")

        # Assert that split_signals1 and split_signals2 have the same number of files
        split_signals1 = os.listdir("data/split_signals1")
        split_signals2 = os.listdir("data/split_signals2")
        self.assertEqual(len(split_signals1), len(split_signals2), "Split signals 1 and 2 have a different number of files")

        # Assert that both split_signals1 and split_signals2 have the same number of files as raw_signals
        self.assertEqual(len(raw_signals), len(split_signals1), "Raw signals and split signals have a different number of files")

        # Clean up
        os.system("rm -rf data/raw_signals")
        os.system("rm -rf data/offset_signals")
        os.system("rm -rf data/substracted_signals")
        os.system("rm -rf data/split_signals1")
        os.system("rm -rf data/split_signals2")
    
    def test_mix_pipeline_threading(self):
        """
        Test running a pipeline with no datahandler or specified Node output, and a datahandler pipeline with output to disk. (Using threading)
        """
        # Run the data generation pipeline and offsets pipeline
        data_gen_pipeline = pipelines.data_generation.data_gen
        data_gen_pipeline.multiprocessing = False
        offset_pipeline = pipelines.offsets_pipeline.offset_pipe
        offset_pipeline.multiprocessing = False
        
        data_gen_pipeline.run()
        offset_pipeline.run()

        # Check if the data was generated
        self.assertTrue(os.path.isdir("data/raw_signals"), "Data was not generated")
        self.assertTrue(os.path.isdir("data/offset_signals"), "Offsets were not generated")
        self.assertTrue(os.path.isdir("data/substracted_signals"), "Substracted signals were not generated")
        self.assertTrue(os.path.isdir("data/split_signals1"), "Split signals 1 were not generated")
        self.assertTrue(os.path.isdir("data/split_signals2"), "Split signals 2 were not generated")

        # Assert that both raw_signals and offsets have the same number of files
        raw_signals = os.listdir("data/raw_signals")
        offset_signals = os.listdir("data/offset_signals")
        self.assertEqual(len(raw_signals), len(offset_signals), "Raw signals and offsets have different a number of files")

        # Assert that both raw_signals and substracted_signals have the same number of files
        substracted_signals = os.listdir("data/substracted_signals")
        self.assertEqual(len(raw_signals), len(substracted_signals), "Raw signals and substracted signals have a different number of files")

        # Assert that split_signals1 and split_signals2 have the same number of files
        split_signals1 = os.listdir("data/split_signals1")
        split_signals2 = os.listdir("data/split_signals2")
        self.assertEqual(len(split_signals1), len(split_signals2), "Split signals 1 and 2 have a different number of files")

        # Assert that both split_signals1 and split_signals2 have the same number of files as raw_signals
        self.assertEqual(len(raw_signals), len(split_signals1), "Raw signals and split signals have a different number of files")

        # Clean up
        os.system("rm -rf data/raw_signals")
        os.system("rm -rf data/offset_signals")
        os.system("rm -rf data/substracted_signals")
        os.system("rm -rf data/split_signals1")
        os.system("rm -rf data/split_signals2")

    def test_mix_pipeline(self):
        """
        Test running a pipeline with no datahandler or specified Node output, and a datahandler pipeline with output to disk. (Single threaded)
        """
        # Run the data generation pipeline and offsets pipeline
        data_gen_pipeline = pipelines.data_generation.data_gen
        data_gen_pipeline.max_workers = 1
        offset_pipeline = pipelines.offsets_pipeline.offset_pipe
        offset_pipeline.max_workers = 1
        
        data_gen_pipeline.run()
        offset_pipeline.run()

        # Check if the data was generated
        self.assertTrue(os.path.isdir("data/raw_signals"), "Data was not generated")
        self.assertTrue(os.path.isdir("data/offset_signals"), "Offsets were not generated")
        self.assertTrue(os.path.isdir("data/substracted_signals"), "Substracted signals were not generated")
        self.assertTrue(os.path.isdir("data/split_signals1"), "Split signals 1 were not generated")
        self.assertTrue(os.path.isdir("data/split_signals2"), "Split signals 2 were not generated")

        # Assert that both raw_signals and offsets have the same number of files
        raw_signals = os.listdir("data/raw_signals")
        offset_signals = os.listdir("data/offset_signals")
        self.assertEqual(len(raw_signals), len(offset_signals), "Raw signals and offsets have different a number of files")

        # Assert that both raw_signals and substracted_signals have the same number of files
        substracted_signals = os.listdir("data/substracted_signals")
        self.assertEqual(len(raw_signals), len(substracted_signals), "Raw signals and substracted signals have a different number of files")

        # Assert that split_signals1 and split_signals2 have the same number of files
        split_signals1 = os.listdir("data/split_signals1")
        split_signals2 = os.listdir("data/split_signals2")
        self.assertEqual(len(split_signals1), len(split_signals2), "Split signals 1 and 2 have a different number of files")

        # Assert that both split_signals1 and split_signals2 have the same number of files as raw_signals
        self.assertEqual(len(raw_signals), len(split_signals1), "Raw signals and split signals have a different number of files")

        # Clean up
        os.system("rm -rf data/raw_signals")
        os.system("rm -rf data/offset_signals")
        os.system("rm -rf data/substracted_signals")
        os.system("rm -rf data/split_signals1")
        os.system("rm -rf data/split_signals2")

    def test_skippy_pipeline_multiprocessing(self):
        """
        Test running a pipeline that skips processing some items. (Using multiprocessing)
        """
        # Run the data generation pipeline and offsets pipeline
        data_gen_pipeline = pipelines.data_generation.data_gen
        data_gen_pipeline.multiprocessing = True
        offset_skippy_pipeline = pipelines.offsets_pipeline.offset_skippy_pipe
        offset_skippy_pipeline.multiprocessing = True
        
        data_gen_pipeline.run()
        offset_skippy_pipeline.run()

        # Check if the data was generated
        self.assertTrue(os.path.isdir("data/raw_signals"), "Data was not generated")
        self.assertTrue(os.path.isdir("data/offset_signals"), "Offsets were not generated")
        self.assertTrue(os.path.isdir("data/substracted_signals"), "Substracted signals were not generated")
        self.assertTrue(os.path.isdir("data/split_signals1"), "Split signals 1 were not generated")
        self.assertTrue(os.path.isdir("data/split_signals2"), "Split signals 2 were not generated")

        # Assert that both raw_signals and offsets have the same number of files
        raw_signals = os.listdir("data/raw_signals")
        offset_signals = os.listdir("data/offset_signals")
        self.assertGreater(len(raw_signals), len(offset_signals), "Raw signals and offsets should have a different number of files, but do not.")

        # Assert that both raw_signals and substracted_signals have the same number of files
        substracted_signals = os.listdir("data/substracted_signals")
        self.assertGreater(len(raw_signals), len(substracted_signals), "Raw signals and substracted signals should have a different number of files, but do not.")

        # Assert that split_signals1 and split_signals2 have the same number of files
        split_signals1 = os.listdir("data/split_signals1")
        split_signals2 = os.listdir("data/split_signals2")
        self.assertEqual(len(split_signals1), len(split_signals2), "Split signals 1 and 2 have a different number of files")

        # Assert that both split_signals1 and split_signals2 have the same number of files as raw_signals
        self.assertGreater(len(raw_signals), len(split_signals1), "Raw signals and split signals should have a different number of files, but do not")

        # Clean up
        os.system("rm -rf data/raw_signals")
        os.system("rm -rf data/offset_signals")
        os.system("rm -rf data/substracted_signals")
        os.system("rm -rf data/split_signals1")
        os.system("rm -rf data/split_signals2")
    
    def test_skippy_pipeline_multithreading(self):
        """
        Test running a pipeline that skips processing some items. (Using multithreading)
        """
        # Run the data generation pipeline and offsets pipeline
        data_gen_pipeline = pipelines.data_generation.data_gen
        data_gen_pipeline.multiprocessing = False
        offset_skippy_pipeline = pipelines.offsets_pipeline.offset_skippy_pipe
        offset_skippy_pipeline.multiprocessing = False
        
        data_gen_pipeline.run()
        offset_skippy_pipeline.run()

        # Check if the data was generated
        self.assertTrue(os.path.isdir("data/raw_signals"), "Data was not generated")
        self.assertTrue(os.path.isdir("data/offset_signals"), "Offsets were not generated")
        self.assertTrue(os.path.isdir("data/substracted_signals"), "Substracted signals were not generated")
        self.assertTrue(os.path.isdir("data/split_signals1"), "Split signals 1 were not generated")
        self.assertTrue(os.path.isdir("data/split_signals2"), "Split signals 2 were not generated")

        # Assert that both raw_signals and offsets have the same number of files
        raw_signals = os.listdir("data/raw_signals")
        offset_signals = os.listdir("data/offset_signals")
        self.assertGreater(len(raw_signals), len(offset_signals), "Raw signals and offsets should have a different number of files, but do not.")

        # Assert that both raw_signals and substracted_signals have the same number of files
        substracted_signals = os.listdir("data/substracted_signals")
        self.assertGreater(len(raw_signals), len(substracted_signals), "Raw signals and substracted signals should have a different number of files, but do not.")

        # Assert that split_signals1 and split_signals2 have the same number of files
        split_signals1 = os.listdir("data/split_signals1")
        split_signals2 = os.listdir("data/split_signals2")
        self.assertEqual(len(split_signals1), len(split_signals2), "Split signals 1 and 2 have a different number of files")

        # Assert that both split_signals1 and split_signals2 have the same number of files as raw_signals
        self.assertGreater(len(raw_signals), len(split_signals1), "Raw signals and split signals should have a different number of files, but do not")

        # Clean up
        os.system("rm -rf data/raw_signals")
        os.system("rm -rf data/offset_signals")
        os.system("rm -rf data/substracted_signals")
        os.system("rm -rf data/split_signals1")
        os.system("rm -rf data/split_signals2")

    def test_skippy_pipeline(self):
        """
        Test running a pipeline that skips processing some items. (Single threaded)
        """
        # Run the data generation pipeline and offsets pipeline
        data_gen_pipeline = pipelines.data_generation.data_gen
        data_gen_pipeline.max_workers = 1
        offset_skippy_pipeline = pipelines.offsets_pipeline.offset_skippy_pipe
        offset_skippy_pipeline.max_workers = 1
        
        data_gen_pipeline.run()
        offset_skippy_pipeline.run()

        # Check if the data was generated
        self.assertTrue(os.path.isdir("data/raw_signals"), "Data was not generated")
        self.assertTrue(os.path.isdir("data/offset_signals"), "Offsets were not generated")
        self.assertTrue(os.path.isdir("data/substracted_signals"), "Substracted signals were not generated")
        self.assertTrue(os.path.isdir("data/split_signals1"), "Split signals 1 were not generated")
        self.assertTrue(os.path.isdir("data/split_signals2"), "Split signals 2 were not generated")

        # Assert that both raw_signals and offsets have the same number of files
        raw_signals = os.listdir("data/raw_signals")
        offset_signals = os.listdir("data/offset_signals")
        self.assertGreater(len(raw_signals), len(offset_signals), "Raw signals and offsets should have a different number of files, but do not.")

        # Assert that both raw_signals and substracted_signals have the same number of files
        substracted_signals = os.listdir("data/substracted_signals")
        self.assertGreater(len(raw_signals), len(substracted_signals), "Raw signals and substracted signals should have a different number of files, but do not.")

        # Assert that split_signals1 and split_signals2 have the same number of files
        split_signals1 = os.listdir("data/split_signals1")
        split_signals2 = os.listdir("data/split_signals2")
        self.assertEqual(len(split_signals1), len(split_signals2), "Split signals 1 and 2 have a different number of files")

        # Assert that both split_signals1 and split_signals2 have the same number of files as raw_signals
        self.assertGreater(len(raw_signals), len(split_signals1), "Raw signals and split signals should have a different number of files, but do not")

        # Clean up
        os.system("rm -rf data/raw_signals")
        os.system("rm -rf data/offset_signals")
        os.system("rm -rf data/substracted_signals")
        os.system("rm -rf data/split_signals1")
        os.system("rm -rf data/split_signals2")

    def test_stop_pipeline_multiprocessing(self):
        """
        Test running a pipeline that stops processing at a random point. (Using multiprocessing)
        """
        # Run the data generation pipeline and offsets pipeline
        data_gen_pipeline = pipelines.data_generation.data_gen
        data_gen_pipeline.multiprocessing = True
        offset_stop_pipeline = pipelines.offsets_pipeline.offset_stop_pipe
        offset_stop_pipeline.multiprocessing = True
        
        data_gen_pipeline.run()
        try:
            offset_stop_pipeline.run()
            self.assertTrue(False, "The pipeline should have raised a StopPipeline exeception before this point")
        except canonada.exceptions.StopPipeline:
            pass

        # Check if the data was generated
        self.assertTrue(os.path.isdir("data/raw_signals"), "Data was not generated")
        self.assertTrue(os.path.isdir("data/offset_signals"), "Offsets were not generated")
        self.assertTrue(os.path.isdir("data/substracted_signals"), "Substracted signals were not generated")
        self.assertTrue(os.path.isdir("data/split_signals1"), "Split signals 1 were not generated")
        self.assertTrue(os.path.isdir("data/split_signals2"), "Split signals 2 were not generated")

        # Assert that both raw_signals and offsets have the same number of files
        raw_signals = os.listdir("data/raw_signals")
        offset_signals = os.listdir("data/offset_signals")
        self.assertGreater(len(raw_signals), len(offset_signals), "Raw signals and offsets should have a different number of files, but do not.")

        # Assert that both raw_signals and substracted_signals have the same number of files
        substracted_signals = os.listdir("data/substracted_signals")
        self.assertGreater(len(raw_signals), len(substracted_signals), "Raw signals and substracted signals should have a different number of files, but do not.")

        # Assert that split_signals1 and split_signals2 have the same number of files
        split_signals1 = os.listdir("data/split_signals1")
        split_signals2 = os.listdir("data/split_signals2")
        self.assertEqual(len(split_signals1), len(split_signals2), "Split signals 1 and 2 have a different number of files")

        # Assert that both split_signals1 and split_signals2 have the same number of files as raw_signals
        self.assertGreater(len(raw_signals), len(split_signals1), "Raw signals and split signals should have a different number of files, but do not")

        # Clean up
        os.system("rm -rf data/raw_signals")
        os.system("rm -rf data/offset_signals")
        os.system("rm -rf data/substracted_signals")
        os.system("rm -rf data/split_signals1")
        os.system("rm -rf data/split_signals2")
    
    def test_stop_pipeline_multithreading(self):
        """
        Test running a pipeline that stops processing at a random point. (Using multithreading)
        """
        # Run the data generation pipeline and offsets pipeline
        data_gen_pipeline = pipelines.data_generation.data_gen
        data_gen_pipeline.multiprocessing = False
        offset_stop_pipeline = pipelines.offsets_pipeline.offset_stop_pipe
        offset_stop_pipeline.multiprocessing = False
        
        data_gen_pipeline.run()
        try:
            offset_stop_pipeline.run()
            self.assertTrue(False, "The pipeline should have raised a StopPipeline exeception before this point")
        except canonada.exceptions.StopPipeline:
            pass

        # Check if the data was generated
        self.assertTrue(os.path.isdir("data/raw_signals"), "Data was not generated")
        self.assertTrue(os.path.isdir("data/offset_signals"), "Offsets were not generated")
        self.assertTrue(os.path.isdir("data/substracted_signals"), "Substracted signals were not generated")
        self.assertTrue(os.path.isdir("data/split_signals1"), "Split signals 1 were not generated")
        self.assertTrue(os.path.isdir("data/split_signals2"), "Split signals 2 were not generated")

        # Assert that both raw_signals and offsets have the same number of files
        raw_signals = os.listdir("data/raw_signals")
        offset_signals = os.listdir("data/offset_signals")
        self.assertGreater(len(raw_signals), len(offset_signals), "Raw signals and offsets should have a different number of files, but do not.")

        # Assert that both raw_signals and substracted_signals have the same number of files
        substracted_signals = os.listdir("data/substracted_signals")
        self.assertGreater(len(raw_signals), len(substracted_signals), "Raw signals and substracted signals should have a different number of files, but do not.")

        # Assert that split_signals1 and split_signals2 have the same number of files
        split_signals1 = os.listdir("data/split_signals1")
        split_signals2 = os.listdir("data/split_signals2")
        self.assertEqual(len(split_signals1), len(split_signals2), "Split signals 1 and 2 have a different number of files")

        # Assert that both split_signals1 and split_signals2 have the same number of files as raw_signals
        self.assertGreater(len(raw_signals), len(split_signals1), "Raw signals and split signals should have a different number of files, but do not")

        # Clean up
        os.system("rm -rf data/raw_signals")
        os.system("rm -rf data/offset_signals")
        os.system("rm -rf data/substracted_signals")
        os.system("rm -rf data/split_signals1")
        os.system("rm -rf data/split_signals2")

    def test_stop_pipeline(self):
        """
        Test running a pipeline that stops processing at a random point. (Single threaded)
        """
        # Run the data generation pipeline and offsets pipeline
        data_gen_pipeline = pipelines.data_generation.data_gen
        data_gen_pipeline.max_workers = 1
        offset_stop_pipeline = pipelines.offsets_pipeline.offset_stop_pipe
        offset_stop_pipeline.max_workers = 1
        
        data_gen_pipeline.run()
        try:
            offset_stop_pipeline.run()
            self.assertTrue(False, "The pipeline should have raised a StopPipeline exeception before this point")
        except canonada.exceptions.StopPipeline:
            pass

        # Check if the data was generated
        self.assertTrue(os.path.isdir("data/raw_signals"), "Data was not generated")
        self.assertTrue(os.path.isdir("data/offset_signals"), "Offsets were not generated")
        self.assertTrue(os.path.isdir("data/substracted_signals"), "Substracted signals were not generated")
        self.assertTrue(os.path.isdir("data/split_signals1"), "Split signals 1 were not generated")
        self.assertTrue(os.path.isdir("data/split_signals2"), "Split signals 2 were not generated")

        # Assert that both raw_signals and offsets have the same number of files
        raw_signals = os.listdir("data/raw_signals")
        offset_signals = os.listdir("data/offset_signals")
        self.assertGreater(len(raw_signals), len(offset_signals), "Raw signals and offsets should have a different number of files, but do not.")

        # Assert that both raw_signals and substracted_signals have the same number of files
        substracted_signals = os.listdir("data/substracted_signals")
        self.assertGreater(len(raw_signals), len(substracted_signals), "Raw signals and substracted signals should have a different number of files, but do not.")

        # Assert that split_signals1 and split_signals2 have the same number of files
        split_signals1 = os.listdir("data/split_signals1")
        split_signals2 = os.listdir("data/split_signals2")
        self.assertEqual(len(split_signals1), len(split_signals2), "Split signals 1 and 2 have a different number of files")

        # Assert that both split_signals1 and split_signals2 have the same number of files as raw_signals
        self.assertGreater(len(raw_signals), len(split_signals1), "Raw signals and split signals should have a different number of files, but do not")

        # Clean up
        os.system("rm -rf data/raw_signals")
        os.system("rm -rf data/offset_signals")
        os.system("rm -rf data/substracted_signals")
        os.system("rm -rf data/split_signals1")
        os.system("rm -rf data/split_signals2")

    def test_error_pipeline_multiprocessing_not_tolerant(self):
        """
        Test running a pipeline that errors at a random point. (Using multiprocessing)
        """
        # Run the data generation pipeline and offsets pipeline
        data_gen_pipeline = pipelines.data_generation.data_gen
        data_gen_pipeline.multiprocessing = True
        offset_error_pipeline = pipelines.offsets_pipeline.offset_error_pipe
        offset_error_pipeline.multiprocessing = True
        offset_error_pipeline.error_tolerant = False
        
        data_gen_pipeline.run()
        try:
            offset_error_pipeline.run()
            self.assertTrue(False, "The pipeline should have raised a RuntimeError exeception before this point")
        except RuntimeError as e:
            pass
        except Exception as e:
            raise e

        # Check if the data was generated
        self.assertTrue(os.path.isdir("data/raw_signals"), "Data was not generated")
        self.assertTrue(os.path.isdir("data/offset_signals"), "Offsets were not generated")
        self.assertTrue(os.path.isdir("data/substracted_signals"), "Substracted signals were not generated")
        self.assertTrue(os.path.isdir("data/split_signals1"), "Split signals 1 were not generated")
        self.assertTrue(os.path.isdir("data/split_signals2"), "Split signals 2 were not generated")

        # Assert that both raw_signals and offsets have the same number of files
        raw_signals = os.listdir("data/raw_signals")
        offset_signals = os.listdir("data/offset_signals")
        self.assertGreater(len(raw_signals), len(offset_signals), "Raw signals and offsets should have a different number of files, but do not.")

        # Assert that both raw_signals and substracted_signals have the same number of files
        substracted_signals = os.listdir("data/substracted_signals")
        self.assertGreater(len(raw_signals), len(substracted_signals), "Raw signals and substracted signals should have a different number of files, but do not.")

        # Assert that split_signals1 and split_signals2 have aproximately the same number of files
        split_signals1 = os.listdir("data/split_signals1")
        split_signals2 = os.listdir("data/split_signals2")
        self.assertLessEqual(len(split_signals1) - len(split_signals2), 1, "Split signals 1 and 2 have a different number of files")

        # Assert that both split_signals1 and split_signals2 have the same number of files as raw_signals
        self.assertGreater(len(raw_signals), len(split_signals1), "Raw signals and split signals should have a different number of files, but do not")

        # Clean up
        os.system("rm -rf data/raw_signals")
        os.system("rm -rf data/offset_signals")
        os.system("rm -rf data/substracted_signals")
        os.system("rm -rf data/split_signals1")
        os.system("rm -rf data/split_signals2")
    
    def test_error_pipeline_multithreading_not_tolerant(self):
        """
        Test running a pipeline that errors at a random point. (Using multithreading)
        """
        # Run the data generation pipeline and offsets pipeline
        data_gen_pipeline = pipelines.data_generation.data_gen
        data_gen_pipeline.multiprocessing = False
        offset_error_pipeline = pipelines.offsets_pipeline.offset_error_pipe
        offset_error_pipeline.multiprocessing = False
        offset_error_pipeline.error_tolerant = False
        
        data_gen_pipeline.run()
        try:
            offset_error_pipeline.run()
            self.assertTrue(False, "The pipeline should have raised a RuntimeError exeception before this point")
        except RuntimeError as e:
            pass
        except Exception as e:
            raise e

        # Check if the data was generated
        self.assertTrue(os.path.isdir("data/raw_signals"), "Data was not generated")
        self.assertTrue(os.path.isdir("data/offset_signals"), "Offsets were not generated")
        self.assertTrue(os.path.isdir("data/substracted_signals"), "Substracted signals were not generated")
        self.assertTrue(os.path.isdir("data/split_signals1"), "Split signals 1 were not generated")
        self.assertTrue(os.path.isdir("data/split_signals2"), "Split signals 2 were not generated")

        # Assert that both raw_signals and offsets have the same number of files
        raw_signals = os.listdir("data/raw_signals")
        offset_signals = os.listdir("data/offset_signals")
        self.assertGreater(len(raw_signals), len(offset_signals), "Raw signals and offsets should have a different number of files, but do not.")

        # Assert that both raw_signals and substracted_signals have the same number of files
        substracted_signals = os.listdir("data/substracted_signals")
        self.assertGreater(len(raw_signals), len(substracted_signals), "Raw signals and substracted signals should have a different number of files, but do not.")

        # Assert that split_signals1 and split_signals2 have the same number of files
        split_signals1 = os.listdir("data/split_signals1")
        split_signals2 = os.listdir("data/split_signals2")
        self.assertEqual(len(split_signals1), len(split_signals2), "Split signals 1 and 2 have a different number of files")

        # Assert that both split_signals1 and split_signals2 have the same number of files as raw_signals
        self.assertGreater(len(raw_signals), len(split_signals1), "Raw signals and split signals should have a different number of files, but do not")

        # Clean up
        os.system("rm -rf data/raw_signals")
        os.system("rm -rf data/offset_signals")
        os.system("rm -rf data/substracted_signals")
        os.system("rm -rf data/split_signals1")
        os.system("rm -rf data/split_signals2")

    def test_error_pipeline_not_tolerant(self):
        """
        Test running a pipeline that errors at a random point. (Single threaded)
        """
        # Run the data generation pipeline and offsets pipeline
        data_gen_pipeline = pipelines.data_generation.data_gen
        data_gen_pipeline.max_workers = 1
        offset_error_pipeline = pipelines.offsets_pipeline.offset_error_pipe
        offset_error_pipeline.max_workers = 1
        offset_error_pipeline.error_tolerant = False
        
        data_gen_pipeline.run()
        try:
            offset_error_pipeline.run()
            self.assertTrue(False, "The pipeline should have raised a RuntimeError exeception before this point")
        except RuntimeError as e:
            pass
        except Exception as e:
            raise e

        # Check if the data was generated
        self.assertTrue(os.path.isdir("data/raw_signals"), "Data was not generated")
        self.assertTrue(os.path.isdir("data/offset_signals"), "Offsets were not generated")
        self.assertTrue(os.path.isdir("data/substracted_signals"), "Substracted signals were not generated")
        self.assertTrue(os.path.isdir("data/split_signals1"), "Split signals 1 were not generated")
        self.assertTrue(os.path.isdir("data/split_signals2"), "Split signals 2 were not generated")

        # Assert that both raw_signals and offsets have the same number of files
        raw_signals = os.listdir("data/raw_signals")
        offset_signals = os.listdir("data/offset_signals")
        self.assertGreater(len(raw_signals), len(offset_signals), "Raw signals and offsets should have a different number of files, but do not.")

        # Assert that both raw_signals and substracted_signals have the same number of files
        substracted_signals = os.listdir("data/substracted_signals")
        self.assertGreater(len(raw_signals), len(substracted_signals), "Raw signals and substracted signals should have a different number of files, but do not.")

        # Assert that split_signals1 and split_signals2 have the same number of files
        split_signals1 = os.listdir("data/split_signals1")
        split_signals2 = os.listdir("data/split_signals2")
        self.assertEqual(len(split_signals1), len(split_signals2), "Split signals 1 and 2 have a different number of files")

        # Assert that both split_signals1 and split_signals2 have the same number of files as raw_signals
        self.assertGreater(len(raw_signals), len(split_signals1), "Raw signals and split signals should have a different number of files, but do not")

        # Clean up
        os.system("rm -rf data/raw_signals")
        os.system("rm -rf data/offset_signals")
        os.system("rm -rf data/substracted_signals")
        os.system("rm -rf data/split_signals1")
        os.system("rm -rf data/split_signals2")

    def test_error_pipeline_multiprocessing_tolerant(self):
        """
        Test running a pipeline that errors at a random point but is error tolerant. (Using multiprocessing)
        """
        # Run the data generation pipeline and offsets pipeline
        data_gen_pipeline = pipelines.data_generation.data_gen
        data_gen_pipeline.multiprocessing = True
        offset_error_pipeline = pipelines.offsets_pipeline.offset_error_pipe
        offset_error_pipeline.multiprocessing = True
        offset_error_pipeline.error_tolerant = True
        
        data_gen_pipeline.run()
        offset_error_pipeline.run()

        # Check if the data was generated
        self.assertTrue(os.path.isdir("data/raw_signals"), "Data was not generated")
        self.assertTrue(os.path.isdir("data/offset_signals"), "Offsets were not generated")
        self.assertTrue(os.path.isdir("data/substracted_signals"), "Substracted signals were not generated")
        self.assertTrue(os.path.isdir("data/split_signals1"), "Split signals 1 were not generated")
        self.assertTrue(os.path.isdir("data/split_signals2"), "Split signals 2 were not generated")

        # Assert that both raw_signals and offsets have the same number of files
        raw_signals = os.listdir("data/raw_signals")
        offset_signals = os.listdir("data/offset_signals")
        self.assertGreater(len(raw_signals), len(offset_signals), "Raw signals and offsets should have a different number of files, but do not.")

        # Assert that both raw_signals and substracted_signals have the same number of files
        substracted_signals = os.listdir("data/substracted_signals")
        self.assertGreater(len(raw_signals), len(substracted_signals), "Raw signals and substracted signals should have a different number of files, but do not.")

        # Assert that split_signals1 and split_signals2 have the same number of files
        split_signals1 = os.listdir("data/split_signals1")
        split_signals2 = os.listdir("data/split_signals2")
        self.assertEqual(len(split_signals1), len(split_signals2), "Split signals 1 and 2 have a different number of files")

        # Assert that both split_signals1 and split_signals2 have the same number of files as raw_signals
        self.assertGreater(len(raw_signals), len(split_signals1), "Raw signals and split signals should have a different number of files, but do not")

        # Clean up
        os.system("rm -rf data/raw_signals")
        os.system("rm -rf data/offset_signals")
        os.system("rm -rf data/substracted_signals")
        os.system("rm -rf data/split_signals1")
        os.system("rm -rf data/split_signals2")
    
    def test_error_pipeline_multithreading_tolerant(self):
        """
        Test running a pipeline that errors at a random point but is error tolerant. (Using multithreading)
        """
        # Run the data generation pipeline and offsets pipeline
        data_gen_pipeline = pipelines.data_generation.data_gen
        data_gen_pipeline.multiprocessing = False
        offset_error_pipeline = pipelines.offsets_pipeline.offset_error_pipe
        offset_error_pipeline.multiprocessing = False
        offset_error_pipeline.error_tolerant = True
        
        data_gen_pipeline.run()
        offset_error_pipeline.run()

        # Check if the data was generated
        self.assertTrue(os.path.isdir("data/raw_signals"), "Data was not generated")
        self.assertTrue(os.path.isdir("data/offset_signals"), "Offsets were not generated")
        self.assertTrue(os.path.isdir("data/substracted_signals"), "Substracted signals were not generated")
        self.assertTrue(os.path.isdir("data/split_signals1"), "Split signals 1 were not generated")
        self.assertTrue(os.path.isdir("data/split_signals2"), "Split signals 2 were not generated")

        # Assert that both raw_signals and offsets have the same number of files
        raw_signals = os.listdir("data/raw_signals")
        offset_signals = os.listdir("data/offset_signals")
        self.assertGreater(len(raw_signals), len(offset_signals), "Raw signals and offsets should have a different number of files, but do not.")

        # Assert that both raw_signals and substracted_signals have the same number of files
        substracted_signals = os.listdir("data/substracted_signals")
        self.assertGreater(len(raw_signals), len(substracted_signals), "Raw signals and substracted signals should have a different number of files, but do not.")

        # Assert that split_signals1 and split_signals2 have the same number of files
        split_signals1 = os.listdir("data/split_signals1")
        split_signals2 = os.listdir("data/split_signals2")
        self.assertEqual(len(split_signals1), len(split_signals2), "Split signals 1 and 2 have a different number of files")

        # Assert that both split_signals1 and split_signals2 have the same number of files as raw_signals
        self.assertGreater(len(raw_signals), len(split_signals1), "Raw signals and split signals should have a different number of files, but do not")

        # Clean up
        os.system("rm -rf data/raw_signals")
        os.system("rm -rf data/offset_signals")
        os.system("rm -rf data/substracted_signals")
        os.system("rm -rf data/split_signals1")
        os.system("rm -rf data/split_signals2")

    def test_error_pipeline_tolerant(self):
        """
        Test running a pipeline that errors at a random point but is error tolerant. (Single threaded)
        """
        # Run the data generation pipeline and offsets pipeline
        data_gen_pipeline = pipelines.data_generation.data_gen
        data_gen_pipeline.max_workers = 1
        offset_error_pipeline = pipelines.offsets_pipeline.offset_error_pipe
        offset_error_pipeline.max_workers = 1
        offset_error_pipeline.error_tolerant = True
        
        data_gen_pipeline.run()
        offset_error_pipeline.run()

        # Check if the data was generated
        self.assertTrue(os.path.isdir("data/raw_signals"), "Data was not generated")
        self.assertTrue(os.path.isdir("data/offset_signals"), "Offsets were not generated")
        self.assertTrue(os.path.isdir("data/substracted_signals"), "Substracted signals were not generated")
        self.assertTrue(os.path.isdir("data/split_signals1"), "Split signals 1 were not generated")
        self.assertTrue(os.path.isdir("data/split_signals2"), "Split signals 2 were not generated")

        # Assert that both raw_signals and offsets have the same number of files
        raw_signals = os.listdir("data/raw_signals")
        offset_signals = os.listdir("data/offset_signals")
        self.assertGreater(len(raw_signals), len(offset_signals), "Raw signals and offsets should have a different number of files, but do not.")

        # Assert that both raw_signals and substracted_signals have the same number of files
        substracted_signals = os.listdir("data/substracted_signals")
        self.assertGreater(len(raw_signals), len(substracted_signals), "Raw signals and substracted signals should have a different number of files, but do not.")

        # Assert that split_signals1 and split_signals2 have the same number of files
        split_signals1 = os.listdir("data/split_signals1")
        split_signals2 = os.listdir("data/split_signals2")
        self.assertEqual(len(split_signals1), len(split_signals2), "Split signals 1 and 2 have a different number of files")

        # Assert that both split_signals1 and split_signals2 have the same number of files as raw_signals
        self.assertGreater(len(raw_signals), len(split_signals1), "Raw signals and split signals should have a different number of files, but do not")

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
