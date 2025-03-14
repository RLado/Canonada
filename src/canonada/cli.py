import sys
import os
import shutil
import tempfile

from graphviz import Digraph # type: ignore

from ._version import __version__
from ._config import config
from ._logger import logger as log
from .catalog import ls as catalog_ls
from .catalog import params as catalog_params
from .pipeline import Pipeline
from .system import System


# Set log level to WARNING for CLI imports
log.setLevel("WARNING")

# Import user defined pipelines
if len(sys.argv) > 1 and sys.argv[1] != "new":
    sys.path.append(os.getcwd())
    try:
        from pipelines import * # type: ignore
    except ImportError as e:
            log.error(e)
            log.error("No pipelines module found in the project directory. Have you initialized a project?")
            raise e

    try:
        from systems import * # type: ignore
    except ImportError as e:
            log.error(e)
            log.error("No systems module found in the project directory. Have you initialized a project?")
            raise e

# Reset the log level
log.setLevel(config.get("logging",{}).get("level", "INFO"))

# Format for the CLI
class Format:
   BOLD = '\033[1m'
   END = '\033[0m'
   UNDERLINE = '\033[4m'

# CLI core function
def cli_core() -> None:
    """
    Core cli functionality. Argument parsing and request execution.
    """

    args = sys.argv
    if len(args) < 2:
        print_usage()
        raise ValueError("No command provided")

    match args[1]:
        case "new":
            if len(args) < 3:
                log.error("No project name provided")
                print_usage()
                raise ValueError("No project name provided")

            # Create a new project
            print(f"Creating new project: {args[2]}")
            create_new_project(args[2])
        
        case "catalog":
            if len(args) < 3:
                log.error("No command provided. Options are 'list' and 'params'")
                print_usage()
                raise ValueError("No command provided")

            match args[2]:
                case "list":
                    # List all available datasets
                    datasets = catalog_ls()
                    print(datasets)

                case "params":
                    # Get the project parameters
                    params = catalog_params()
                    print(params)

                case _:
                    log.error("Command not recognized. Options are 'list' and 'params'")
                    print_usage()

        case "registry":
            if len(args) < 3:
                log.error("No command provided. Options are 'pipelines' and 'systems'")
                print_usage()
                raise ValueError("No command provided")

            match args[2]:
                case "pipelines":
                    # List all available pipelines
                    for pipeline in Pipeline.registry:
                        if pipeline.description != "":
                            if len(pipeline.description.splitlines()) > 1:
                                print(f"{Format.BOLD}{pipeline.name}{Format.END}: {pipeline.description.splitlines()[0]} [...]")
                            else:
                                print(f"{Format.BOLD}{pipeline.name}{Format.END}: {pipeline.description.splitlines()[0]}")
                        else:
                            print(pipeline.name)

                case "systems":
                    # List all available systems
                    for system in System.registry:
                        if system.description != "":
                            if len(system.description.splitlines()) > 1:
                                print(f"{Format.BOLD}{system.name}{Format.END}: {system.description.splitlines()[0]} [...]")
                            else:
                                print(f"{Format.BOLD}{system.name}{Format.END}: {system.description.splitlines()[0]}")
                        else:
                            print(system.name)

                case _:
                    log.error("Command not recognized. Options are 'pipelines' and 'systems'")
                    print_usage()       
            
        case "run":
            if len(args) < 4:
                log.error("No pipeline(s) or system(s) name provided")
                print_usage()
                raise ValueError("No pipeline(s) or system(s) name provided")
            
            # Run requested pipeline(s) or system(s)
            match args[2]:
                case "pipelines":
                    for pipeline in args[3:]:
                        ran = False
                        for p in Pipeline.registry:
                            if p.name == pipeline:
                                p()
                                ran = True
                                break
                        if not ran:
                            log.error(f"Pipeline {pipeline} not found")

                case "systems":
                    for system in args[3:]:
                        for s in System.registry:
                            ran = False
                            if s.name == system:
                                s()
                                ran = True
                                break
                        if not ran:
                            log.error(f"System {system} not found")

                case _:
                    log.error("Command not recognized. Options are 'pipelines' and 'systems'")      
                    print_usage()
                    raise ValueError ("Command not recognized")

        case "view":
            if len(args) < 4:
                log.error("No pipeline or system name provided")
                print_usage()
                raise ValueError("No pipeline or system name provided")
            
            # Visualize requested pipeline or system
            match args[2]:
                case "pipelines":
                    for pipeline in args[3:]:
                        viewed = False
                        for p in Pipeline.registry:
                            if p.name == pipeline:
                                print(p)
                                graph = visualize_pipeline(p)
                                graph.view(os.path.join(tempfile.gettempdir(), f"{p.name}_pipeline_graph"), cleanup=True)
                                viewed = True
                                break
                        if not viewed:
                            log.error(f"Pipeline {pipeline} not found")

                
                case "systems":
                    for system in args[3:]:
                        viewed = False
                        for s in System.registry:
                            if s.name == system:
                                print(s)
                                viewed = True
                                break
                        if not viewed:
                            log.error(f"System {system} not found")
                        
                case _:
                    log.error("Command not recognized. Options are 'pipelines' and 'systems'")      
                    print_usage()
                    raise ValueError("Command not recognized")

        case "version":
            # Print the version of the package
            print(f"Canonada version: {__version__}")

        case _:
            log.error("Command not recognized")
            print_usage()
            raise ValueError("Command not recognized")


def create_new_project(name: str) -> None:
    """
    Build the directory structure and files for a new project

    Args:
        name (str): The name of the project
    """

    # Create project directories
    dirs = [
        "config",
        "data",
        "datahandlers",
        "notebooks",
        "pipelines",
        "pipelines/nodes",
        "systems",
        "tests",
    ]

    for dir in dirs:
        os.makedirs(dir, exist_ok=True)
    
    # Create project files
    files = [
        "gitignore",
        "canonada.toml",
        "config/catalog.toml",
        "config/parameters.toml",
        "config/credentials.toml",
        "datahandlers/__init__.py",
        "pipelines/__init__.py",
        "pipelines/nodes/__init__.py",
        "systems/__init__.py",
    ]

    for file in files:
        # Check if the file already exists
        if os.path.exists(file):
            log.error(f"File {file} already exists")
            raise ValueError(f"File {file} already exists")

        # Copy from the package to the project
        shutil.copyfile(os.path.join(os.path.dirname(__file__), "templates", file), file)

        # Special cases
        match file:
            # If the file copied was gitignore, rename it to .gitignore
            case "gitignore":
                os.rename(file, ".gitignore")

            case "canonada.toml":
                # Add name of the project to the canonada.toml file
                with open("canonada.toml", "r") as f:
                    content = f.read()
                    content = content.replace("$project_name", name)
                # Write the updated content back to the file
                with open("canonada.toml", "w") as f:
                    f.write(content)   

def print_usage() -> None:
    """
    Print the usage string for the CLI
    """
    print("""
Usage: canonada <command> <args>
Commands:
    new <project_name> - Create a new project
    catalog [list/params] - List all available datasets or get the project parameters
    registry [pipelines/systems] - List all available pipelines or systems
    run [pipelines/systems] <name(s)> - Run a pipeline or system
    view [pipelines/systems] <name(s)> - View a pipeline or system
    version - Print the version of Canonada
    
""")

def visualize_pipeline(pipeline: Pipeline) -> Digraph:
    """
    Produce a graphviz SVG graph given a `Pipeline` object
    """

    # Load catalog data
    catlg = catalog_ls()
    params = catalog_params()

    # Define a new Diagraph object
    dot = Digraph(
        name=pipeline.name,
        comment=pipeline.description,
        format='svg',
        graph_attr={
            'rankdir': 'TB',  # LR (horizontal) TB (vertical)
            'nodesep': '0.2',  # Vertical spacing between nodes
            'ranksep': '1.0',  # Horizontal spacing between ranks
        }
    )
    
    # Add all nodes
    for node in pipeline.nodes:
        # Add sources
        for inpt in node.input:
            if inpt in catlg:
                dot.node(
                    name=inpt,
                    label=inpt,
                    shape='ellipse',
                    style='filled',
                    fillcolor='#ffebcc',
                    fontsize='10'
                )
        # Add sinks
        for output in node.output:
            if output in catlg:
                dot.node(
                    name=output,
                    label=output,
                    shape='ellipse',
                    style='filled',
                    fillcolor='#ccffcc',
                    fontsize='10'
                )

        # Format each node input and output
        formatted_inputs = []
        formatted_outputs = []

        # Process inputs - check for params: prefix
        for inpt in node.input:
            if inpt.startswith('params:'):
                param_name = inpt[7:]  # Remove 'params:' prefix
                if param_name in params:
                    # Truncate long values for display (10 characters)
                    param_value = str(params[param_name])[:10]
                    if len(str(params[param_name])) > 10:
                        param_value += "..."
                    formatted_inputs.append(f"{inpt} ({param_value})")
                else:
                    formatted_inputs.append(inpt)
            else:
                formatted_inputs.append(inpt)
                
        # Process outputs
        for output in node.output:
            formatted_outputs.append(output)
            
        # Replace the input/output sections in the label with formatted versions
        node_inputs_str = '<br/>'.join(formatted_inputs)
        node_outputs_str = '<br/>'.join(formatted_outputs)
        

        # Add processing nodes
        label = (
            f"<<table border='0' cellborder='0' cellpadding='0' align='center'>"
            f"<tr><td colspan='2' align='center'><b>{node.name}</b></td></tr>"
            f"<tr><td colspan='2' align='center'><i>{node.description}</i></td></tr>"
            f"<tr>"
            f"<td align='left'><b>Inputs:</b> {node_inputs_str}</td>"
            f"<td align='right'><b>Outputs:</b> {node_outputs_str}</td>"
            f"</tr>"
            f"</table>>"
        )
        dot.node(
            name=node.name,
            label=label,
            shape='rectangle',
            style='filled',
            fillcolor='#e6f3ff',
            fontsize='10'
        )
    
    # Create connections
    for node in pipeline.nodes:
        # Add edges from sources to nodes
        for inpt in node.input:
            if inpt in catlg:
                dot.edge(inpt, node.name, label=inpt, fontsize='10')

        # Add edges from nodes to consumers
        for output in node.output:
            consumers = [
                n.name for n in pipeline.nodes
                if output in n.input and n.name != node.name
            ]
            for consumer in consumers:
                dot.edge(node.name, consumer, label=output, fontsize='10')

        # Add edges from nodes to sinks
        for output in node.output:
            if output in catlg:
                dot.edge(node.name, output, fontsize='10')
    
    return dot

# CLI entrypoint
def main() -> None:
    """
    Canonada's CLI entrypoint

    Adds a layer of error management to cli_core
    """

    try:
        cli_core()
    except Exception as e:
        log.error(e)
        sys.exit(1)


if __name__ == "__main__":
    main()