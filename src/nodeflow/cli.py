import sys
import os
import shutil

from ._version import __version__
from .logger import logger as log

def main():
    args = sys.argv

    match args[1]:
        case "new":
            if len(args) < 3:
                log.error("No project name provided")
                sys.exit(1)

            # Create a new project
            print(f"Creating new project: {args[2]}")
            create_new_project(args[2])

        case "run":
            if len(args) < 3:
                log.error("No pipeline(s) or system(s) name provided")
                sys.exit(1)
            
            # Run requested pipeline(s) or system(s)
            log.info("Running pipeline or system (under development)")

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
    run <pipeline/system> - Run a pipeline or system
    visualize <pipeline/system> - Visualize a pipeline or system
    docs - Generate and serve documentation
    version - Print the version of NodeFlow
    
""")

if __name__ == "__main__":
    main()