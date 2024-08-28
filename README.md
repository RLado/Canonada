## Canonada
> ⚠️ Canonada is currently under development and is not ready for production use. 

Canonada is a data science framework that helps you build production-ready streaming pipelines for data processing in Python.

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
- **Visualize your data pipeline**: Visualize your data pipeline
- **Documentation**: Collect and display the documentation of your project [⚠️ under development]

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
    docs - Generate and serve documentation [not implemented]
    version - Print the version of Canonada
```

## Installation
**TO DO**

## Documentation
**TO DO**

## Contributing
**TO DO**