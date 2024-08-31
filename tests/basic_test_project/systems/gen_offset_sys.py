import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../src"))
from canonada.system import System

# Import the pipelines
import pipelines.data_generation
import pipelines.offsets_pipeline


# Define a system running the pipelines sequentially
gen_offset_sys = System("gen_offset_sys", [
    pipelines.data_generation.data_gen,
    pipelines.offsets_pipeline.offset_pipe,
])

