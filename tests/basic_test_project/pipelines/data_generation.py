import os
import sys

from .nodes import signal_generator

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../src"))
from canonada.pipeline import Node, Pipeline


data_gen = Pipeline("data_generation",
    [
        Node(
            func=signal_generator.gen,
            input=["params:sig_gen.num_signals", "params:sig_gen.num_samples"],
            output=[], # No usable outputs, writes to disk (data/raw_signals)
            name="signal_generator"
        ),
    ]
)