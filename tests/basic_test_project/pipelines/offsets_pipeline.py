import os
import sys
import secrets

from .nodes_offset import test_nodes

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../src"))
from canonada.pipeline import Node, Pipeline
from canonada.exceptions import SkipItem, StopPipeline


offset_nodes = [
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
]

offset_pipe = Pipeline("offset_pipe", offset_nodes, error_tolerant = False)


# Define a pipeline to test item skipping using the SkipItem exception
def rnd_skip(dummy):
    rnd_num = secrets.randbits(53) / (1 << 53)
    if rnd_num > 0.5:
        return None
    else:
        raise SkipItem(message="Some message")

skippy_node = Node(
    func=rnd_skip,
    input=["raw_signals"],
    output=["_"],
    name="skip_randomly"
)

offset_skippy_pipe = Pipeline("offset_skippy_pipe", [skippy_node] + offset_nodes, error_tolerant = False)

# Define a pipeline with a StopPipeline at random
def rnd_stop(dummy):
    rnd_num = secrets.randbits(53) / (1 << 53)
    if rnd_num > 0.1:
        return None
    else:
        raise StopPipeline(message="Some final message")

stop_node = Node(
    func=rnd_stop,
    input=["raw_signals"],
    output=["_"],
    name="stop_randomly"
)

offset_stop_pipe = Pipeline("offset_stop_pipe", [stop_node] + offset_nodes, error_tolerant = False)

# Define a pipeline with an induced RuntimeError at random
def rnd_error(dummy):
    rnd_num = secrets.randbits(53) / (1 << 53)
    if rnd_num > 0.1:
        return None
    else:
        raise RuntimeError("Some error message")

error_node = Node(
    func=rnd_error,
    input=["raw_signals"],
    output=["_"],
    name="error_randomly"
)

offset_error_pipe = Pipeline("offset_error_pipe", [error_node] + offset_nodes, error_tolerant = False)