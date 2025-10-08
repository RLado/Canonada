"""
Canonada

A data science framework that helps you build production-ready streaming pipelines for data processing in Python
"""

import multiprocessing
import platform
import sys

from . import catalog as catalog
from . import exceptions as exceptions
from . import pipeline as pipeline
from . import system as system

# Python 3.14 changed its default multiprocessing start method from "fork" to "forkserver", breaking functionality
if sys.version_info >= (3, 14):
    # Set fork on supported platforms (Linux)
    if platform.system() == "Linux":
        multiprocessing.set_start_method("fork", force=True)
