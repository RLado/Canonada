import os
import tomllib
from typing import Any

from .._logger import logger as log
from ._datahandlers import Datahandler, check_datahandler, available_datahandlers


def get(dataset_name: str) -> Datahandler:
    """
    Get a datahandler from the catalog by dataset name.

    Args:
        dataset_name (str): The name of the dataset to get.

    Returns:
        Any: The datahandler object.
    """

    # Check if the catalog file exists
    if not os.path.isfile("config/catalog.toml"):
        raise FileNotFoundError("Catalog file not found")
        return
    
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

    # Check if the catalog file exists
    if not os.path.isfile("config/catalog.toml"):
        raise FileNotFoundError("Catalog file not found")
        return
    
    # Read the catalog file
    catalog: dict
    with open("config/catalog.toml", "rb") as f:
        catalog = tomllib.load(f)

    # List all available datasets
    return list(catalog.keys())

def params() -> dict[str, Any]:
    """
    Get parameters.

    Returns:
        dict: A dictionary with the project's parameters.
    """

    # Check if the parameters file exists
    if not os.path.isfile("config/parameters.toml"):
        raise FileNotFoundError("Parameters file not found")
        return
    
    # Read the parameters file
    params: dict
    with open("config/parameters.toml", "rb") as f:
        params = tomllib.load(f)
    
    # Flatten dictionary  
    params = _flatten(params)

    return params

def credentials() -> dict[str, Any]:
    """
    Get credentials.

    Returns:
        dict: A dictionary with the project's credentials.
    """

    # Check if the credentials file exists
    if not os.path.isfile("config/credentials.toml"):
        raise FileNotFoundError("Credentials file not found")
        return
    
    # Read the credentials file
    cred: dict
    with open("config/credentials.toml", "rb") as f:
        cred = tomllib.load(f)
    
    # Flatten dictionary  
    cred = _flatten(cred)

    return cred

def _flatten(d, parent_key='', sep='.') -> dict[str, Any]:
    """
    Flatten a dictionary. Nested keys are concatenated with a dot.

    Args:
        d (dict): The dictionary to flatten.
        parent_key (str): The parent key.
        sep (str): The separator to use.
    
    Returns:
        dict: The flattened dictionary guaranteed to have only one level of keys.
    """

    items: list = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(_flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))

    return dict(items)