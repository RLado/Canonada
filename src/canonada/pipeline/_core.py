import io
import traceback
from concurrent.futures import ThreadPoolExecutor

from ..logger import logger as log
from ..catalog import ls as catalog_ls
from ..catalog import get as catalog_get
from ..catalog import params as catalog_params
from ..catalog import Datahandler


class Node():
    """
    Node data structure for pipeline construction.
    """

    registry = []

    @classmethod
    def ls(cls):
        """
        List all available nodes
        """
        return cls.registry

    def __init__(self, name:str, input:list[str], output:list[str], func:callable, description:str="") -> None:
        """
        Instanciate a new node.

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
        self.func: callable = func

        # Check that the node name is unique and not empty
        if self.name == "":
            raise ValueError("Node name cannot be empty")
        if self.name in [node.name for node in Node.registry]:
            raise ValueError(f"Node name '{self.name}' is not unique")

        assert len(set(input)) == len(input), "Input list contains duplicates"
        assert len(set(output)) == len(output), "Output list contains duplicates"
        assert callable(self.func), "Function is not callable"
    
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
    registry = []

    @classmethod
    def ls(cls):
        """
        List all available pipelines
        """
        return cls.registry

    def __init__(self, name:str, nodes:list[Node], description:str="") -> None:
        """
        Instanciate a new pipeline.

        Args:
            name (str): The name of the pipeline. This name will be used to call the pipeline from the command line. The name must be unique.
            nodes (list[Node]): The list of nodes in the pipeline.
            description (str, optional): A description of the pipeline. Defaults to "".
        """

        self.name:str = name
        self.description:str = description
        self.nodes:list[Node] = nodes
        self.exec_order:list[Node] = []
        self.input_datahandlers:dict[str, Datahandler] = {}
        self.output_datahandlers:dict[str, Datahandler] = {}
        self.max_workers: int = None

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
            repr_buffer.write("---------------------------------\n\n")

        repr_buffer.write("\n".join([str(node) for node in self.nodes]))
        repr_buffer.write("\n")

        return repr_buffer.getvalue()
    
    def __call__(self) -> None:
        """
        Run the pipeline
        """
        self.run()

    def _calc_exec_order(self) -> None:
        """
        Calculate the execution order of the nodes. Get the necessary datahandlers for input and output.
        """
        log.debug("Calculating pipeline execution order")

        # Reset the execution order (avoid duplicates)
        self.exec_order = []
        self.input_datahandlers = {}
        self.output_datahandlers = {}

        # Check that no outputs are repeated
        outputs: set = set()
        for node in self.nodes:
            for output in node.output:
                if output in outputs:
                    raise ValueError(f"The pipeline contains multiple nodes with the same output: {output}")
                outputs.add(output)
        
        # Check which outouts are in the catalog
        catalog_outputs: set = set()
        for output in outputs:
            if output in catalog_ls():
                catalog_outputs.add(output)
        
        # Make sure that no outputs are parameters
        params = {f"params:{key}" for key in catalog_params()}
        if len(catalog_outputs.intersection(params)) > 0:
            raise ValueError(f"Output to a node cannot be a parameter: {catalog_outputs.intersection(params)}")

        # Check that no outputs can be known inputs
        known_inputs: set = set()
        for node in self.nodes:
            for input in node.input:
                if input in catalog_ls():
                    known_inputs.add(input)
        
        for known_input in known_inputs:
            if known_input in outputs:
                raise ValueError(f"Output '{known_input}' is also an input. This is not allowed.")

        # Get the necessary datahandlers for input and output
        for input in known_inputs:
            self.input_datahandlers[input] = catalog_get(input)
        for output in catalog_outputs:
            self.output_datahandlers[output] = catalog_get(output)
        
        # Add the parameters to the known inputs to calculate the execution order
        known_inputs.update(params)
        
        # Calculate the execution order
        nodes_to_process = self.nodes.copy()
        nodes_idx_processed = []
        # Start with the nodes that have no inputs until there are none left
        for i, node in enumerate(nodes_to_process):
            if len(node.input) == 0:
                self.exec_order.append(node)
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
                    self.exec_order.append(node)
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
    
    def run_once(self, known_inputs:dict[str, any]) -> dict[str, any]:
        """
        Run the pipeline once with known inputs.

        Args:
            known_inputs (dict[str, any]): A dictionary with known inputs.

        Returns:
            dict[str, any]: A dictionary with the known outputs.
        """

        # Calculate the execution order & get datahandlers
        self._calc_exec_order()

        # Read the project parameters
        params = catalog_params()

        # Add parameters to the known inputs
        known_inputs.update({f"params:{key}": value for key, value in params.items()})

        # Execute the nodes in order
        for node in self.exec_order:
            log.debug(f"Running node: {node.name} for inputs: {known_inputs.keys()}")
            # Prepare the inputs for the node
            node_inputs = [known_inputs[input_name] for input_name in node.input]
            # Run the node
            output_data = node.func(*node_inputs)
            # If the node does not return a tuple, check if a list/tuple is returned and wrap it in a tuple
            if not isinstance(output_data, tuple) and not isinstance(output_data, list):
                output_data = (output_data,)
            if len(output_data) != len(node.output):
                if len(node.output) == 1:
                    output_data = (output_data,)
            # Update the known inputs
            known_inputs.update({output: output_data[i] for i, output in enumerate(node.output)})
        
        return known_inputs # Now being the known outputs       
        
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
        if len(self.input_datahandlers) == 0:
            self.run_once({})
            log.info(f"Pipeline {self.name} finished")
            return

        # From the first node in the exec_order, get the first cataloged datasource
        master_datahandler: str
        for input_src in self.exec_order[0].input:
            if input_src in self.input_datahandlers:
                master_datahandler = input_src
                break

        def run_pass(master: tuple[tuple, any]):
            """
            Run a single pass of the pipeline

            Args:
                master (tuple[tuple, any]): A tuple with the master key and the master data

            Returns:
                None
            """
            master_key, _ = master

            try:
                known_inputs = params.copy()
                for input_name, datahandler in self.input_datahandlers.items():
                    known_inputs[input_name] = datahandler[master_key]
                    
                # Execute the nodes in order
                for node in self.exec_order:
                    # Prepare the inputs for the node
                    node_inputs = [known_inputs[input_name] for input_name in node.input]
                    # Run the node
                    output_data = node.func(*node_inputs)
                    # If the node does not return a tuple, check if a list/tuple is returned and wrap it in a tuple
                    if not isinstance(output_data, tuple) and not isinstance(output_data, list):
                        output_data = (output_data,)
                    if len(output_data) != len(node.output):
                        if len(node.output) == 1:
                            output_data = (output_data,)
                    # Update the known inputs
                    known_inputs.update({output: output_data[i] for i, output in enumerate(node.output)})
                    # Check if the output data should be saved
                    for output_name in node.output:
                        if output_name in self.output_datahandlers:
                            self.output_datahandlers[output_name].save(known_inputs[output_name])
                            
            except Exception as e:
                log.error(f"Error in pipeline {self.name} with key {master_key}: {e}")
                log.error(traceback.format_exc())
                raise e

        # Get a key for the first datahandler and use the key to retrieve all other input data for the pipeline
        with ThreadPoolExecutor(max_workers=self.max_workers) as mpool:
            mpool.map(run_pass, self.input_datahandlers[master_datahandler])            
        
        log.info(f"Pipeline {self.name} finished")