import sys
import os
import shutil
import tomllib

from ._version import __version__
from .logger import logger as log
from .catalog import ls as catalog_ls
from .catalog import params as catalog_params
from .pipeline import Pipeline

# Read nodeflow.toml
config: dict
try:
    with open("nodeflow.toml", "rb") as f:
        config = tomllib.load(f)
except FileNotFoundError:
    config = {
        'logging': {
            'level': 'INFO'
        }
    }

# Set log level to WARNING for CLI imports
log.setLevel("WARNING")

# Import user defined pipelines
sys.path.append(os.getcwd())
try:
    from pipelines import *
except ImportError:
    log.error("No pipelines module found in the project directory. Have you initialized a project?")

# Reset the log level
log.setLevel(config['logging']['level'])

def main():
    args = sys.argv
    if len(args) < 2:
        print_usage()
        sys.exit(1)

    match args[1]:
        case "new":
            if len(args) < 3:
                log.error("No project name provided")
                sys.exit(1)

            # Create a new project
            print(f"Creating new project: {args[2]}")
            create_new_project(args[2])
        
        case "catalog":
            if len(args) < 3:
                log.error("No command provided. Options are 'list' and 'params'")
                sys.exit(1)

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
                    sys.exit(1)  

        case "registry":
            if len(args) < 3:
                log.error("No command provided. Options are 'pipelines' and 'systems'")
                sys.exit(1)

            match args[2]:
                case "pipelines":
                    # List all available pipelines
                    for pipeline in Pipeline.registry:
                        print(pipeline.name)

                case "systems":
                    # List all available systems
                    log.info("Listing all available systems (under development)")

                case _:
                    log.error("Command not recognized. Options are 'pipelines' and 'systems'")
                    print_usage()
                    sys.exit(1)          
            
        case "run":
            if len(args) < 4:
                log.error("No pipeline(s) or system(s) name provided")
                sys.exit(1)
            
            # Run requested pipeline(s) or system(s)
            match args[2]:
                case "pipelines":
                    for pipeline in args[3:]:
                        for p in Pipeline.registry:
                            if p.name == pipeline:
                                p()
                                break
                        else:
                            log.error(f"Pipeline {pipeline} not found")
                            sys.exit(1)

                case "systems":
                    log.info("Running system (under development)")

                case _:
                    log.error("Command not recognized. Options are 'pipelines' and 'systems'")      
                    print_usage()
                    sys.exit(1)  

        case "visualize":
            if len(args) < 3:
                log.error("No pipeline or system name provided")
                sys.exit(1)
            
            # Visualize requested pipeline or system
            log.info("Visualizing pipeline or system (under development)")

        case "docs":
            # Generate and serve documentation
            log.info("Generating and serving documentation (under development)")

        case "version":
            # Print the version of the package
            print(f"NodeFlow version: {__version__}")

        case _:
            log.error("Command not recognized")
            print_usage()
            sys.exit(1)


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
        "notebooks",
        "nodes",
        "pipelines",
        "systems",
        "tests",
    ]

    for dir in dirs:
        os.makedirs(dir, exist_ok=True)
    
    # Create project files
    files = [
        "gitignore",
        "nodeflow.toml",
        "config/catalog.toml",
        "config/parameters.toml",
        "config/credentials.toml",
        "nodes/__init__.py",
        "pipelines/__init__.py",
        "systems/__init__.py",
    ]

    for file in files:
        # Check if the file already exists
        if os.path.exists(file):
            log.error(f"File {file} already exists")
            sys.exit(1)

        # Copy from the package to the project
        shutil.copyfile(os.path.join(os.path.dirname(__file__), "templates", file), file)

        # Special cases
        match file:
            # If the file copied was gitignore, rename it to .gitignore
            case "gitignore":
                os.rename(file, ".gitignore")

            case "nodeflow.toml":
                # Add name of the project to the nodeflow.toml file
                with open("nodeflow.toml", "r") as f:
                    content = f.read()
                    content = content.replace("$project_name", name)
                # Write the updated content back to the file
                with open("nodeflow.toml", "w") as f:
                    f.write(content)   

def print_usage() -> None:
    """
    Print the usage string for the CLI
    """
    print("""
Usage: nodeflow <command> <args>
Commands:
    new <project_name> - Create a new project
    catalog <list/params> - List all available datasets or get the project parameters
    registry <pipelines/systems> - List all available pipelines or systems
    run <pipeline/system> <name> - Run a pipeline or system
    visualize <pipeline/system> - Visualize a pipeline or system [not implemented]
    docs - Generate and serve documentation [not implemented]
    version - Print the version of NodeFlow
    
""")

if __name__ == "__main__":
    main()