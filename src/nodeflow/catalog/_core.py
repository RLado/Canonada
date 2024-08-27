import os
import sys
import tomllib

from ..logger import logger as log
from ._datahandlers import Datahandler, check_datahandler, available_datahandlers

# Import user defined pipelines
sys.path.append(os.getcwd())
try:
    from datahandlers import available_datahandlers as user_available_datahandlers
except ImportError as e:
    log.error(e)
    log.error("No datahandlers module found in the project directory. Have you initialized a project?")
    sys.exit(1)

# Add user defined datahandlers to the available datahandlers
available_datahandlers.update(user_available_datahandlers)


def get(dataset_name: str) -> Datahandler:
    """
    Get a datahandler from the catalog by dataset name.

    Args:
        dataset_name (str): The name of the dataset to get.

    Returns:
        Any: The datahandler object.
    """
    
    # Read the catalog file
    catalog: dict
    with open("config/catalog.toml", "rb") as f:
        catalog = tomllib.load(f)

    # Search for the specified dataset
    dh_type = catalog[dataset_name]["type"]

    if dh_type not in available_datahandlers:
        raise ValueError(f"Dataset type '{dh_type}' not found")
        return
    
    # Create the datahandler
    datahandler = available_datahandlers[dh_type](dataset_name, catalog[dataset_name]["keys"], catalog[dataset_name])

    if not check_datahandler(datahandler):
        log.warning(f"Datahandler '{dh_type}' does not comply with the datahandler interface. This may cause isssues.")
    
    return datahandler

def ls() -> list:
    """
    List all available datasets in the catalog.

    Returns:
        list: A list of available datasets.
    """
    
    # Read the catalog file
    catalog: dict
    with open("config/catalog.toml", "rb") as f:
        catalog = tomllib.load(f)

    # List all available datasets
    return list(catalog.keys())

def params() -> dict[str, any]:
    """
    Get parameters.

    Returns:
        dict: A dictionary with the project's parameters.
    """
    
    # Read the parameters file
    catalog: dict
    with open("config/parameters.toml", "rb") as f:
        catalog = tomllib.load(f)

    return catalog