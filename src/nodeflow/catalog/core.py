import tomllib

from ..logger import logger as log
from .datahandlers import Datahandler, check_datahandler, available_datahandlers

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
    dh_type = catalog[dataset_name]["type"].split(".")

    if dh_type[0].strip() == "nodeflow":
        if dh_type[1] not in available_datahandlers:
            raise ValueError(f"Dataset type '{dh_type[1]}' not found")
            return
        
        # Create the datahandler
        datahandler = available_datahandlers[dh_type[1]](dataset_name, catalog[dataset_name]["keys"], catalog[dataset_name])

        if not check_datahandler(datahandler):
            log.warning(f"Datahandler '{dh_type[1]}' does not comply with the datahandler interface. This may cause isssues.")
        
        return datahandler

    else:
        raise ValueError(f"Dataset type {dh_type} not supported")
    
    return

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