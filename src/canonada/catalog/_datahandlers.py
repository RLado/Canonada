import json
import os
import uuid
from pathlib import Path
from ..logger import logger as log


class Datahandler():
    """
    Base class for all datahandlers. Datahandlers are used to load and save datasets and must be capable of streaming data.

    Datahandlers can be given keys to build an index with, this will be necessary for nodes that load from multiple datasets at once. If no keys are provided, the specific implementations of datahandlers will be responsible for building the index or erroring out.
    """

    registry = []

    @classmethod
    def ls(cls):
        """
        List all available datahandlers
        """
        return cls.registry

    def __init__(self, name: str, dh_type: str, keys: list, kwargs: dict):
        self.name = name
        self.type = dh_type
        self.keys = keys
        self.kwargs = kwargs
        self.index = {}

        # Check that the datahandler type is unique and not empty
        if self.type == "":
            raise ValueError("Datahandler name cannot be empty")
        if self.type in [dh.type for dh in Datahandler.registry]:
            raise ValueError(f"Datahandler type '{self.type}' is not unique")

        log.info(f"Initializing datahandler '{self.type}' for '{self.name}'")

        # Register the datahandler
        Datahandler.registry.append(self)
    
    def __len__(self):
        return len(self.index)

    def __iter__(self):
        for key, file in self.index.items():
            yield key, self._load(file)
    
    def __getitem__(self, key: str):
        return self._load(self.index[key])
    
    def _load(self, file: str) -> dict:
        """
        Load a single file from the dataset.

        Args:
            file (str): Path to the file to load.
        """
        raise NotImplementedError("Datahandler must implement the '_load' method.")
    
    def save(self, kwargs: dict) -> None:
        """
        Save data to a file.

        Args:
            kwargs (dict): The data to save.
        """
        raise NotImplementedError("Datahandler must implement the 'save' method.")

def check_datahandler(datahandler: Datahandler) -> bool:
    """
    Check if a given datahandler class implements the minimum required methods.

    Args:
        dataset (Any): A dataset class.

    Returns:
        bool: A boolean indicating if the dataset class is valid.
    """

    # Check that is a class
    if not isinstance(datahandler, Datahandler):
        log.warning("Datahandler is not a class")
        return False
    
    # ---
    
    # Check if the minimum required attributes are present
    if not hasattr(datahandler, "name"):
        log.warning("Datahandler has no 'name' attribute")
        return False

    if not hasattr(datahandler, "keys"):
        log.warning("Datahandler has no 'keys' attribute")
        return False

    if not hasattr(datahandler, "kwargs"):
        log.warning("Datahandler has no 'kwargs' attribute")
        return False

    if not hasattr(datahandler, "index"):
        log.warning("Datahandler has no 'index' attribute")
        return False

    # ---

    # Check if the minimum required methods are implemented
    if not callable(getattr(datahandler, "__iter__", None)):
        log.warning("Datahandler has no '__iter__' method")
        return False

    if not callable(getattr(datahandler, "__getitem__", None)):
        log.warning("Datahandler has no '__getitem__' method")
        return False
    
    if not callable(getattr(datahandler, "__len__", None)):
        log.warning("Datahandler has no '__len__' method")
        return False

    if not callable(getattr(datahandler, "save", None)):
        log.warning("Datahandler has no 'save' method")
        return False
    
    # ---

    return True

class JsonMulti(Datahandler):
    """
    Loads a multi JSON file dataset, returning files as dictionaries one by one.

    If no keys are provided, the index will be built using the filenames as keys.
    """

    def __init__(self, name: str, keys: set, kwargs: dict):
        super().__init__(name, "canonada.json_multi", keys, kwargs)
        if "path" not in kwargs:
            raise ValueError("No path provided for json_multi datahandler.")
        self.path = kwargs["path"]

        # List all files
        files: list[Path] = [p for p in Path( self.path ).rglob('*.json')]

        self.index = {}
        # Read files and build an index with the given keys
        if len(keys) == 0: # If no keys are provided, use the filenames
            for file in files:
                # Strip preceding path and extension
                filename = file.stem
                self.index[filename] = file
        else:
            for file in files:
                keys_values = []
                data = self._load(file)
                for key in self.keys:
                    if key in data:
                        keys_values.append(data[key])
                    else:
                        log.warning(f"Key '{key}' not found in file '{file}'. Defaulting to None.")
                        keys_values.append(None)
                
                # Warning if the key values are not unique
                if tuple(keys_values) in self.index:
                    log.warning(f"Key values {keys_values} are not unique. Dropping file '{file}'.")
                    continue

                # Add the file to the index
                self.index[tuple(keys_values)] = file

        return
    
    def _load(self, file: str) -> dict:
        """
        Load a single file from the dataset.

        Args:
            file (str): Path to the file to load.
        """

        with open(file, "r") as f:
            return json.load(f)

    def save(self, kwargs:dict) -> None:
        """
        Save data to a json file. Files are saved in the root path of the dataset.

        If the file already exists, it will be overwritten.

        Args:
            kwargs (dict): List of keyword arguments. Required arguments:
                - filename (str): The filename to save the data to.
                - data (dict): The data to save in json format.
        """
        # Check if kwargs is a dict and contains the required keys
        if not isinstance(kwargs, dict):
            raise ValueError("Invalid format provided provided to JsonMulti. Expected dict with 'filename' and 'data' keys.")
        # If no filename or data is provided, assume the provided dict is the data
        if "filename" not in kwargs or "data" not in kwargs:
            data = kwargs.copy()
            kwargs={"filename": None, "data": data}
        # If no filename is provided, generate a random one
        if kwargs["filename"] is None:
            kwargs["filename"] = str(uuid.uuid4())

        with open(os.path.join(self.path, f"{kwargs["filename"]}.json"), "w") as f:
            json.dump(kwargs["data"], f)
        
# Register of all built in datasets
available_datahandlers = {
    "canonada.json_multi": JsonMulti
}