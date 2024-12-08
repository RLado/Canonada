## Canonada
> ⚠️ Canonada is currently under development. 

Canonada is a data science framework that helps you build production-ready streaming pipelines for data processing in Python.

[![GitHub branch check runs](https://img.shields.io/github/check-runs/rlado/canonada/master)](https://github.com/RLado/Canonada)
[![PyPI - Version](https://img.shields.io/pypi/v/canonada)](https://pypi.org/project/canonada/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/canonada)](https://pypi.org/project/canonada/)

## Why Canonada?
- **Standardized**: Canonada provides a standardized way to build your data projects
- **Modular**: Canonada is modular and allows you to build and visualize data pipelines with ease
- **Memory Efficient**: Canonada is memory efficient and can handle large datasets by streaming data through the pipeline instead of loading it all at once

## Features
- **Centralized control of data sources**: Manage all your data sources in one place, enabling you to keep your team in sync
- **Centralized control of the project configuration**: Manage all your project configurations in one place
- **Easy dataloading**: Load data from various sources like CSV, JSON, Parquet, etc.
- **Use functions as nodes**: Functions are the building blocks of Canonada. You can use any function as a node in your pipeline
- **Create streaming data pipelines**: Create parallel and sequential data pipelines with ease
- **Visualize your data pipeline**: Visualize your data pipelines, nodes and connections

## Project Structure
```
canonada.toml
config/
    catalog.toml
    parameters.toml
    credentials.toml
data/
    ...
datahandlers/
    __init__.py
    custom_datahandler_1.py
    custom_datahandler_2.py
    ...
notebooks/
    ...
pipelines/
    __init__.py
    pipeline_1.py
    pipeline_2.py
    nodes_1/
        __init__.py
        node_1.py
        node_2.py
        ...
    nodes_2/
        __init__.py
        node_3.py
        node_4.py
        ...
    ...
systems/
    __init__.py
    system_1.py
    system_2.py
    ...
tests/
    test_node_group_1.py
    test_node_group_2.py
    ...
```

## Usage
Available commands:
```
Usage: canonada <command> <args>
Commands:
    new <project_name> - Create a new project
    catalog [list/params] - List all available datasets or get the project parameters
    registry [pipelines/systems] - List all available pipelines or systems
    run [pipelines/systems] <name(s)> - Run a pipeline or system
    view [pipelines/systems] <name(s)> - View a pipeline or system
    version - Print the version of Canonada
```

## Installation
Canonada is available on [PyPI](https://pypi.org/project/canonada/) and can be installed using pip:
```bash
pip install canonada
```

> Check out the [Getting Started](https://github.com/RLado/Canonada/wiki/GettingStarted) guide to learn how to create a new project with Canonada.

## Documentation
Check out the project's documentation [here](https://github.com/RLado/Canonada/wiki)

## Contributing
Contributions are welcome! If you have any suggestions, examples, datahandlers, bug reports, or feature requests, please open an issue or a discussion thread.