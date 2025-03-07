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
        name="update_signal_1"
        ),
    Node(
        func=test_nodes.update_signal, 
        input=["raw_signals", "offsets"], 
        output=["offset_signals_mem"], 
        name="update_signal_2"
        ),
    Node(
        func=test_nodes.split_signal, 
        input=["offset_signals_mem"], 
        output=["signal1", "signal2"], 
        name="split_signal"
        ),
    Node(
        func=test_nodes.substract_signals, 
        input=["signal1", "signal2"], 
        output=["substracted_signals"], 
        name="substract_signals"
        ),
    Node(
        func=test_nodes.split_signal,
        input=["offset_signals_mem"],
        output=["split_signals1", "split_signals2"],
        name="split_signal_and_save"
    )
])