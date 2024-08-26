from ..logger import logger as log
from ..catalog import ls as catalog_ls
from ..catalog import get as catalog_get
from ..catalog import Datahandler

class Node():
    """
    Node data structure for pipeline construction.
    """
    def __init__(self, name:str, input:list[str], output:list[str], func:callable):
        self.name:str = name
        self.input: list = input
        self.output: list = output
        self.func:func = func

        assert len(set(input)) == len(input), "Input list contains duplicates"
        assert len(set(output)) == len(output), "Output list contains duplicates"
        assert callable(self.func), "Function is not callable"
    
    def __repr__(self):
        """
        Show node name and input/output
        """
        return f"Node: {self.name} | Input: {self.input} | Output: {self.output}"
    
class Pipeline():
    """
    Pipeline data structure for nodeflow construction.
    """
    def __init__(self, name:str, nodes:list[Node]):
        self.name:str = name
        self.nodes:list[Node] = nodes
        self.exec_order:list[Node] = []
        self.input_datahandlers:dict[str, Datahandler] = {}
        self.output_datahandlers:dict[str, Datahandler] = {}

        # Calculate the execution order & get datahandlers
        self._calc_exec_order()
    
    def __repr__(self):
        """
        Show all nodes in the pipeline
        """
        return "\n".join([str(node) for node in self.nodes])
    
    def __call__(self):
        """
        Run the pipeline
        """
        self.run()

    def _calc_exec_order(self):
        """
        Calculate the execution order of the nodes. Get the necessary datahandlers for input and output.
        """
        log.debug("Calculating pipeline execution order")

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
        
        # Calculate the execution order
        nodes_to_process = self.nodes.copy()
        nodes_idx_processed = []
        # Start with the nodes that have no inputs until there are none left
        for i, node in enumerate(nodes_to_process):
            if len(node.input) == 0:
                self.exec_order.append(node)
                known_inputs.add(*node.output)
                nodes_idx_processed.append(i)
        
        # Remove the processed nodes from the nodes_to_process list
        for idx in sorted(nodes_idx_processed, reverse=True):
            nodes_to_process.pop(idx)
        
        # Process the rest of the nodes
        max_iter = len(self.nodes)
        i = 0
        while len(nodes_to_process) > 0:            
            nodes_idx_processed = []
            # Get all nodes with exclusively known inputs
            for i, node in enumerate(nodes_to_process):
                if set(node.input).issubset(known_inputs):
                    self.exec_order.append(node)
                    known_inputs.add(*node.output)
                    nodes_idx_processed.append(i)
            
            # Remove the processed nodes from the nodes_to_process list
            for idx in sorted(nodes_idx_processed, reverse=True):
                nodes_to_process.pop(idx)
            
            # Increment the iteration counter (avoid infinite loop)
            i += 1
            if i > max_iter:
                raise ValueError("Pipeline contains a cycle")
                break
        
    def run(self):
        """
        Execute the pipeline
        """
        log.info(f"Running pipeline: {self.name}")

        # From the first node in the exec_order, get the first catalogged datasource
        master_datahandler: str
        for input_src in self.exec_order[0].input:
            if input_src in self.input_datahandlers:
                master_datahandler = input_src
                break

        # Get a key for the first datahandler and use the key to retireve all other input data for the pipeline
        for master_key, _ in self.input_datahandlers[master_datahandler]:
            known_inputs = {}
            for input_name, datahandler in self.input_datahandlers.items():
                known_inputs[input_name] = datahandler[master_key]
                
            # Execute the nodes in order
            for node in self.exec_order:
                log.debug(f"Running node: {node.name} for inputs: {known_inputs.keys()}")
                # Prepare the inputs for the node
                node_inputs = [known_inputs[input_name] for input_name in node.input]
                # Run the node
                output_data = node.func(*node_inputs)
                # If the node does not return a tuple, check if a list/tuple is returned and wrap it in a tuple
                # Check if output_data implements len
                if not hasattr(output_data, "__len__"):
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
        
        log.info(f"Pipeline {self.name} finished")