import copy
import io
import multiprocessing
import threading
import traceback
from typing import Any, Callable

from .._config import config
from .._logger import logger as log
from .._utils.progressbar import ProgressBar
from ..catalog import Datahandler
from ..catalog import get as catalog_get
from ..catalog import ls as catalog_ls
from ..catalog import params as catalog_params
from ..exceptions import SkipItem, StopPipeline


class _ThreadReturn(threading.Thread):
    """
    Custom Thread object that allows a value return on .join
    """

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        threading.Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)

    def join(self, *args):
        super().join(*args)
        return self._return

class _ProcessReturn(multiprocessing.Process):
    """
    Custom Process object that allows a value to return on .join
    """

    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, daemon=None):
        self._q = multiprocessing.Queue(maxsize=1)  # For result or exception
        super().__init__(group=group, target=target, name=name, args=args, kwargs=kwargs, daemon=daemon)

    def run(self):
        if self._target is not None:
            result = self._target(*self._args, **self._kwargs)
        else:
            result = None
        self._q.put(result, block=True)

    def join(self, *args):
        super().join(*args)
        if self.is_alive():
            return None  
        
        # Process has finished
        if not self._q.empty():
            return self._q.get_nowait()
        return None

class Node():
    """
    Node data structure for pipeline construction.
    """

    registry: list = []

    @classmethod
    def ls(cls):
        """
        List all available nodes
        """

        return cls.registry

    def __init__(self, name:str, input:list[str], output:list[str], func:Callable, description:str="") -> None:
        """
        Instantiate a new node.

        Args:
            name (str): The name of the node.
            input (list[str]): The list of input arguments to the node given as strings.
            output (list[str]): The list of output arguments to the node given as strings.
            func (callable): The function to be executed by the node.
            description (str, optional): A description of the node. Defaults to "".
        """
        
        self.name:str = name
        self.description:str = description
        self.input: list = input
        self.output: list = output
        self.func: Callable = func

        # Check that the node name is unique and not empty
        if self.name == "":
            raise ValueError("Node name cannot be empty")
        if self.name in [node.name for node in Node.registry]:
            raise ValueError(f"Node name '{self.name}' is not unique")

        assert len(set(input)) == len(input), "Input list contains duplicates"
        assert len(set(output)) == len(output), "Output list contains duplicates"
        assert callable(self.func), "Function is not callable"

        # Register the node
        Node.registry.append(self)
    
    def __repr__(self) -> str:
        """
        Show node name and input/output
        """

        repr_buffer = io.StringIO()
        repr_buffer.write(f"Node: {self.name}\n\tinput: {self.input}\n\toutput: {self.output}")
        if self.description != "":
            repr_buffer.write(f"\n\tdescription: {self.description}")

        return repr_buffer.getvalue()
    
class Pipeline():
    """
    Pipeline data structure for canonada construction.
    """

    registry: list = []

    @classmethod
    def ls(cls):
        """
        List all available pipelines
        """

        return cls.registry

    def __init__(self, name:str, nodes:list[Node], description:str="", max_workers:int|None=None, multiprocessing:bool=True, error_tolerant:bool=True) -> None:
        """
        Instantiate a new pipeline.

        Args:
            name (str): The name of the pipeline. This name will be used to call the pipeline from the command line. The name must be unique.
            nodes (list[Node]): The list of nodes in the pipeline.
            description (str, optional): A description of the pipeline. Defaults to "".
            max_workers (int, optional): The maximum number of workers to use. Defaults to None (uses all available cores).
            multiprocessing (bool, optional): Whether to use multiprocessing. Defaults to True.
            error_tolerant (bool, optional): If an error occurs inside the pipeline does not stop its execution. Defaults to True.
        """

        self.name:str = name
        self.description:str = description
        self.nodes:list[Node] = nodes
        self.max_workers: int|None = max_workers
        self.multiprocessing: bool = multiprocessing
        self.error_tolerant: bool = error_tolerant
        self._exec_order:list[Node] = []
        self._input_datahandlers:dict[str, Datahandler] = {}
        self._output_datahandlers:dict[str, Datahandler] = {}

        # Check that the pipeline name is unique and not empty
        if self.name == "":
            raise ValueError("Pipeline name cannot be empty")

        if self.name in [pipe.name for pipe in Pipeline.registry]:
            raise ValueError(f"Pipeline name '{self.name}' is not unique")

        # Register the pipeline
        Pipeline.registry.append(self)
    
    def __repr__(self) -> str:
        """
        Show all nodes in the pipeline
        """

        repr_buffer = io.StringIO()

        repr_buffer.write(f"----- Pipeline: {self.name} -----\n")
        if self.description != "":
            repr_buffer.write(f"Description: {self.description}\n")
            repr_buffer.write(f"{'-'*(22 + len(self.name))}\n\n") # Make the separator as long as the title bar

        repr_buffer.write("\n".join([str(node) for node in self.nodes]))
        repr_buffer.write("\n")

        return repr_buffer.getvalue()
    
    def __call__(self) -> None:
        """
        Run the pipeline
        """

        self.run()

    def _calc_exec_order(self, known_inputs: set[str] = set(), init_datahandlers: bool = True) -> None:
        """
        Calculate the execution order of the nodes. Get the necessary datahandlers for input and output.

        Args:
            known_inputs (set[str], optional): A set of known inputs to the pipeline. Useful for pipelines designed to be ran
              only using the `run_once` method. Defaults to an empty set.
            init_datahandlers (bool, optional): Whether to initialize the datahandlers for input and output. Defaults to True.
        """

        log.debug("Calculating pipeline execution order")

        # Reset the execution order (avoid duplicates)
        self._exec_order = []
        self._input_datahandlers = {}
        self._output_datahandlers = {}

        # Check that no outputs are repeated
        outputs: set = set()
        for node in self.nodes:
            for output in node.output:
                if output in outputs:
                    raise ValueError(f"The pipeline contains multiple nodes with the same output: {output}")
                outputs.add(output)
        
        # Check which outputs are in the catalog
        catalog_outputs: set = set()
        for output in outputs:
            if output in catalog_ls():
                catalog_outputs.add(output)
        
        # Make sure that no outputs are parameters
        params = {f"params:{key}" for key in catalog_params()}
        if len(catalog_outputs.intersection(params)) > 0:
            raise ValueError(f"Output to a node cannot be a parameter: {catalog_outputs.intersection(params)}")

        # Check that no outputs can be known inputs
        known_inputs = set([ki for ki in known_inputs if ki[:8] != "params:"]) # Remove parameters from `known_inputs`
        for node in self.nodes:
            for input in node.input:
                if input in catalog_ls():
                    known_inputs.add(input)
        
        for known_input in known_inputs:
            if known_input in outputs:
                raise ValueError(f"Output '{known_input}' is also an input. This is not allowed.")

        # Get the necessary datahandlers for input and output (if required)
        if init_datahandlers:
            for input in known_inputs:
                self._input_datahandlers[input] = catalog_get(input)
            for output in catalog_outputs:
                self._output_datahandlers[output] = catalog_get(output)
        
        # Add the parameters to the known inputs to calculate the execution order
        known_inputs.update(params)
        
        # Calculate the execution order
        nodes_to_process = self.nodes.copy()
        nodes_idx_processed = []
        # Start with the nodes that have no inputs until there are none left
        for i, node in enumerate(nodes_to_process):
            if len(node.input) == 0:
                self._exec_order.append(node)
                if len(node.output) > 0:
                    known_inputs.update(node.output)
                nodes_idx_processed.append(i)
        
        # Remove the processed nodes from the nodes_to_process list
        for idx in sorted(nodes_idx_processed, reverse=True):
            nodes_to_process.pop(idx)
        
        # Process the rest of the nodes
        max_iter = len(self.nodes)
        iter_count = 0
        while len(nodes_to_process) > 0:  
            nodes_idx_processed = []
            # Get all nodes with exclusively known inputs
            for i, node in enumerate(nodes_to_process):
                if set(node.input).issubset(known_inputs):
                    self._exec_order.append(node)
                    if len(node.output) > 0:
                        known_inputs.update(node.output)
                    nodes_idx_processed.append(i)
            
            # If no nodes were processed, the pipeline does not have enough inputs to run
            if len(nodes_idx_processed) == 0:
                nodes_string = "\n\t".join([str(node) for node in nodes_to_process])
                raise ValueError("Pipeline does not have enough inputs to run completely. Make sure all parameters are defined."
                                 f"\n -- Nodes left to be processed: \n{nodes_string}")                  
                break

            # Remove the processed nodes from the nodes_to_process list
            for idx in sorted(nodes_idx_processed, reverse=True):
                nodes_to_process.pop(idx)
            
            # Increment the iteration counter (avoid infinite loop)
            iter_count += 1
            if iter_count > max_iter:
                raise ValueError("Pipeline contains a cycle")
                break
        
        # Log a warning for those outputs that are never used as inputs or saved
        inputs: set[str] = set([i for node in self.nodes for i in node.input])
        for o in outputs:
            if o not in catalog_outputs and o not in inputs:
                log.warning(f"Output named '{o}' is never used nor saved.")
    
    def run_once(self, known_inputs:dict[str, Any]) -> dict[str, Any]:
        """
        Run the pipeline once with known inputs.

        Args:
            known_inputs (dict[str, any]): A dictionary with known inputs.

        Returns:
            dict[str, any]: A dictionary with the known outputs.
        """

        # Calculate the execution order & get datahandlers
        if self._exec_order == []:
            self._calc_exec_order(known_inputs=set(known_inputs.keys()), init_datahandlers=False)

        # Read the project parameters
        params = catalog_params()

        # Add parameters to the known inputs
        known_inputs.update({f"params:{key}": value for key, value in params.items()})

        # Execute the nodes in order
        for node in self._exec_order:
            log.debug(f"Running node: {node.name} for inputs: {known_inputs.keys()}")
            # Prepare the inputs for the node
            node_inputs = [copy.deepcopy(known_inputs[input_name]) for input_name in node.input]
            # Run the node
            output_data = node.func(*node_inputs)
            # If the node does not return a tuple, check if a list/tuple is returned and wrap it in a tuple
            if not isinstance(output_data, tuple):
                output_data = (output_data,)
            if len(output_data) != len(node.output):
                if len(node.output) == 1:
                    output_data = (output_data,)
            # Update the known inputs
            known_inputs.update({output: output_data[i] for i, output in enumerate(node.output)})
        
        return known_inputs # Now being the known outputs       
    
    # Define the function to run a single pass of the pipeline
    def _run_pass(self, master: tuple[tuple, Any], params: dict[str, Any]) -> None|Exception:
        """
        Run a single pass of the pipeline

        Args:
            master (tuple[tuple, any]): A tuple with the master key and the master data
            params (dict[str, any]): Catalog parameters dictionary

        Returns:
            None|Exception
        """

        master_key, _ = master

        try:
            known_inputs = params
            for input_name, datahandler in self._input_datahandlers.items():
                known_inputs[input_name] = datahandler[master_key]
                
            # Execute the nodes in order
            for node in self._exec_order:
                # Prepare the inputs for the node
                node_inputs = [copy.deepcopy(known_inputs[input_name]) for input_name in node.input]
                # Run the node
                output_data = node.func(*node_inputs)
                # If the node does not return a tuple, check if a list/tuple is returned and wrap it in a tuple
                if not isinstance(output_data, tuple):
                    output_data = (output_data,)
                if len(output_data) != len(node.output):
                    if len(node.output) == 1:
                        output_data = (output_data,)
                    else:
                        log.error(f"Node '{node.name}' is producing more outputs ({len(output_data)}) than declared ({len(node.output)})")
                        raise RuntimeError(f"Node '{node.name}' is producing more outputs ({len(output_data)}) than declared ({len(node.output)})")
                # Update the known inputs
                known_inputs.update({output: output_data[i] for i, output in enumerate(node.output)})
                # Check if the output data should be saved
                for output_name in node.output:
                    if output_name in self._output_datahandlers:
                        self._output_datahandlers[output_name].save(known_inputs[output_name])

        except SkipItem as e:
            e.master_key = master_key
            log.debug(e)
            return None
        except StopPipeline as e:
            return StopPipeline(master_key = master_key, message = e.message)
        except Exception as e:
            log.error(f"Error in pipeline {self.name} with key {master_key}: {e}\n{traceback.format_exc()}")
            if not self.error_tolerant:
                return e
        return None

    def run(self) -> None:
        """
        Execute the pipeline
        """

        # Calculate the execution order & get datahandlers
        self._calc_exec_order()
        
        log.info(f"Running pipeline: {self.name}")

        # Read the project parameters
        params = catalog_params()
        params = {f"params:{key}": value for key, value in params.items()}

        # If none of the pipeline inputs are datahandlers, run the pipeline once
        if len(self._input_datahandlers) == 0:
            self.run_once({})
            log.info(f"Pipeline {self.name} finished")
            return

        # From the first node in the exec_order, get the first cataloged datasource
        master_datahandler: str
        for input_src in self._exec_order[0].input:
            if input_src in self._input_datahandlers:
                master_datahandler = input_src
                break

        # Adjust the number of workers if not set
        if self.max_workers is None:
            self.max_workers = multiprocessing.cpu_count()

        # Create a progress bar (if configured)
        show_prog = config.get("logging",{}).get("show_progress", True)
        if show_prog:
            prog_bar = ProgressBar(total=len(self._input_datahandlers[master_datahandler]), width=30, prefix=f"Pipeline {self.name}:")

        if self.max_workers < 1:
            raise ValueError("Number of workers must be greater than 0. Set to None to use all available cores.")

        # Start pipeline execution
        if show_prog:
            prog_bar.update(0)

        if self.max_workers == 1:
            # Run the pipeline sequentially with no threading or multiprocessing
            for mkey in self._input_datahandlers[master_datahandler]:
                try:
                    res = self._run_pass(mkey, params)
                    if res:
                        raise res
                except StopPipeline as e:
                    log.error(e)
                    raise e
                except Exception as e:
                    raise e
                if show_prog:
                    prog_bar.update()

        elif not self.multiprocessing:
            # Start multithreaded pipeline execution
            # Create a master key iterator
            mkey_iter = iter(self._input_datahandlers[master_datahandler])

            # Define and fill a thread pool
            thread_pool = []
            for _ in range(self.max_workers):
                try:
                    mkey = next(mkey_iter)
                    thread = _ThreadReturn(target=self._run_pass, args=(mkey, params))
                    thread.start()
                    thread_pool.append(thread)
                except StopIteration:
                    break
            
            # Wait for threads to finish and start new ones until the input data is exhausted
            while len(thread_pool) > 0:
                for thread in thread_pool:
                    if not thread.is_alive():
                        try:
                            res = thread.join(0)
                            if res:
                                raise res
                        except StopPipeline as e:
                            log.error(e)
                            raise e
                        except Exception as e:
                            raise e
                        thread_pool.remove(thread)
                        if show_prog:
                            prog_bar.update()
                        try:
                            mkey = next(mkey_iter)
                            thread = _ThreadReturn(target=self._run_pass, args=(mkey, params))
                            thread.start()
                            thread_pool.append(thread)
                        except StopIteration:
                            break

        else:
            # Start multiprocessed pipeline execution
            # Create a master key iterator
            mkey_iter = iter(self._input_datahandlers[master_datahandler])

            # Define and fill a process pool
            process_pool = []
            for _ in range(self.max_workers):
                try:
                    mkey = next(mkey_iter)
                    process = _ProcessReturn(target=self._run_pass, args=(mkey, params))
                    process.start()
                    process_pool.append(process)
                except StopIteration:
                    break
            
            # Wait for processes to finish and start new ones until the input data is exhausted
            while len(process_pool) > 0:
                for process in process_pool:
                    if not process.is_alive():
                        try:
                            res = process.join(0)
                            if res:
                                raise res
                        except StopPipeline as e:
                            log.error(e)
                            raise e
                        except Exception as e:
                            for p in process_pool: # Kill remaining processes
                                p.kill()
                            raise e
                        process_pool.remove(process)
                        if show_prog:
                            prog_bar.update()
                        try:
                            mkey = next(mkey_iter)
                            process = _ProcessReturn(target=self._run_pass, args=(mkey, params))
                            process.start()
                            process_pool.append(process)
                        except StopIteration:
                            break

        # Finish the progress bar
        if show_prog:
            prog_bar.finish()

        log.info(f"Pipeline {self.name} finished")