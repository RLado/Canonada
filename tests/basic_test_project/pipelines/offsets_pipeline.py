import os
import sys

from .nodes_offset import test_nodes

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../src"))
from canonada.pipeline import Node, Pipeline

offset_pipe = Pipeline("offset_pipe", [
    Node(
        func=test_nodes.create_offsets, 
        input=["raw_signals", "params:offset_signal.random_seed"], 
        output=["offsets"], 
        name="create_offsets"
        ),
    Node(
        func=test_nodes.update_signal, 
        input=["raw_signals", "offsets"], 
        output=["offset_signals"], 
        name="update_signal"
        ),
])