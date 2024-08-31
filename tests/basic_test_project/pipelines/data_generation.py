import os
import sys

from .nodes_data_gen import signal_generator

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../src"))
from canonada.pipeline import Node, Pipeline


data_gen = Pipeline("data_generation",
    [
        # Generated signals are written to disk
        Node(
            func=signal_generator.gen,
            input=["params:sig_gen.num_signals", "params:sig_gen.num_samples"],
            output=[], # No usable outputs, writes to disk (data/raw_signals)
            name="signal_generator"
        ),
    ]
)