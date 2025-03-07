import csv
import json
import os
import uuid
from pathlib import Path
import time
import fcntl
from typing import Any, Generator

from .._logger import logger as log


class Datahandler():
    """
    Base class for all datahandlers. Datahandlers are used to load and save datasets and must be capable of streaming data.

    Datahandlers can be given keys to build an index with, this will be necessary for nodes that load from multiple datasets at once. If no keys are provided, the specific implementations of datahandlers will be responsible for building the index or erroring out.
    """

    registry: list = []

    @classmethod
    def ls(cls) -> list:
        """
        List all available datahandlers
        """
        return cls.registry

    def __init__(self, name: str, dh_type: str, keys: set, kwargs: dict) -> None:
        """
        Instantiate a new datahandler.

        Args:
            name (str): The name of the datahandler.
            dh_type (str): The type of the datahandler. This is used to identify the datahandler implementation in the catalog.
            keys (list): A list of keys to build an index with.
            kwargs (dict): A dictionary of keyword arguments to be used by the datahandler.
        """

        self.name = name
        self.type = dh_type
        self.keys = keys
        self.kwargs = kwargs
        self.index: dict[str|tuple, Any] = {}

        # Check that the datahandler type is not empty
        if self.type == "":
            raise ValueError("Datahandler name cannot be empty")

        log.debug(f"Initializing datahandler '{self.type}' for '{self.name}'")

        # Register the datahandler
        Datahandler.registry.append(self)
    
    def __len__(self) -> int:
        return len(self.index)

    def __iter__(self) -> Generator[tuple[Any, dict], Any, None]:
        for key, file in self.index.items():
            yield key, self._load(file)
    
    def __getitem__(self, key: str|tuple) -> dict:
        return self._load(self.index[key])
    
    def _load(self, file: Path) -> dict:
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

    def __init__(self, name: str, keys: set, kwargs: dict) -> None:
        """
        Instantiate a new canonada.json_multi datahandler.

        Args:
            name (str): The name of the datahandler. Used to identify the datahandler in the catalog.
            keys (set): A set of keys to build the index with.
            kwargs (dict): A dictionary of keyword arguments to be used by the datahandler. Required arguments:
                - path (str): The path to the dataset.
        """

        super().__init__(name, "canonada.json_multi", keys, kwargs)
        if "path" not in kwargs:
            raise ValueError("No path provided for json_multi datahandler.")
        self.path = kwargs["path"]

        # Check if the path exists, if not, create it
        if not os.path.isdir(self.path):
            log.warning(f"Path '{self.path}' not found for json_multi datahandler. Creating it.")
            os.makedirs(self.path)
            return # No need to load data if the path is empty

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
    
    def _load(self, file: Path) -> dict:
        """
        Load a single file from the dataset.

        Args:
            file (str): Path to the file to load.
        """

        with open(file, "r") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError as e:
                log.error(f"Error loading file '{file}': {e}")
                return {}

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

        with open(os.path.join(self.path, f"{kwargs['filename']}.json"), "w") as f:
            json.dump(kwargs["data"], f)

class CSVRows(Datahandler):
    """
    Loads and indexes a CSV file by rows. The first row is considered the header.
    """
    
    def __init__(self, name: str, keys: set, kwargs: dict) -> None:
        """
        Instantiate a new canonada.csv_rows datahandler.

        Args:
            name (str): The name of the datahandler. Used to identify the datahandler in the catalog.
            keys (set): A set of keys to build the index with.
            kwargs (dict): A dictionary of keyword arguments to be used by the datahandler. Required arguments:
                - path (str): The path to the dataset file.
                - headers (list, optional): A list of headers for the dataset. Recommended if the file is expected to be empty.
        """

        super().__init__(name, "canonada.csv_rows", keys, kwargs)
        if "path" not in kwargs:
            raise ValueError("No path provided for csv_datahandler.")
        self.path = kwargs["path"]

        # Load the headers
        if "headers" in kwargs:
            self.headers = kwargs["headers"]
        else:
            self.headers = []

        # Check if the file exists
        if not os.path.isfile(self.path):
            log.warning(f"File {self.path} not found for csv_rows datahandler. Creating an empty file.")
            if len(self.headers) == 0:
                log.warning("No headers provided for csv_rows datahandler. Creating an empty file.")
            with open(self.path, 'w') as f:
                f.write(",".join(self.headers))
                f.write("\n")
            return
        
        # Load the data and create the index
        data = self._load(self.path)

        # Create an index with the given keys or with row index
        if len(keys) == 0:
            self.index = data
        else:
            for row in data.values():
                key = tuple(row[k] for k in keys)
                if key in self.index:
                    log.warning(f"Key {key} is not unique. Dropping row.")
                    continue
                self.index[key] = row

    def __len__(self) -> int:
        return len(self.index)
    
    def __iter__(self) -> Generator[tuple[str | tuple, Any], Any, None]:
        for key, row in self.index.items():
            yield key, row
    
    def __getitem__(self, key) -> Any:
        return self.index[key]
    
    def _load(self, file) -> dict:
        with open(file, 'r') as f:
            reader = csv.DictReader(f, skipinitialspace=True)
            self.header = reader.fieldnames
            return {i: row for i, row in enumerate(reader)}
    
    def save(self, kwargs: dict) -> None:
        """
        Save data the dataset file.

        Args:
            kwargs (dict): The data to save. The keys must match the headers.
        """

        if "path" not in self.kwargs:
            raise ValueError("No path provided for csv_rows.")

        with open(self.kwargs["path"], 'a') as f:
            while True:
                try:
                    fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)  # Try to acquire a lock
                    break  # Lock acquired
                except BlockingIOError:
                    time.sleep(0.1)  # Wait if the file is already open
                    log.debug("Waiting for file lock")

            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writerow(kwargs)
            fcntl.flock(f, fcntl.LOCK_UN)  # Release the lock


# Register of all built in datasets
available_datahandlers = {
    "canonada.json_multi": JsonMulti,
    "canonada.csv_rows": CSVRows,
}
