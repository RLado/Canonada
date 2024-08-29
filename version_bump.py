"""
Bump up the version number of the package
"""

import tomllib
import sys

if __name__ == "__main__":
    # Read pyproject.toml
    config: dict
    with open("pyproject.toml", "rb") as f:
        config = tomllib.load(f)

    version = config["project"]["version"]
    major, minor, patch = version.split(".")

    # Interface
    args = sys.argv
    if len(args) < 2:
        # Bump up the patch version
        patch = int(patch) + 1
    else:
        match args[1]:
            case "major":
                major = int(major) + 1
                minor = 0
                patch = 0
            case "minor":
                minor = int(minor) + 1
                patch = 0
            case "patch":
                patch = int(patch) + 1
            case _:
                print("Invalid argument")
                raise ValueError("Invalid argument")
    
    new_version = f"{major}.{minor}.{patch}"

    # Open pyproject.toml for writing
    contents: str
    with open("pyproject.toml", "r") as f:
        contents = f.read()
        contents = contents.replace(f'version = "{version}"', f'version = "{new_version}"')
    with open("pyproject.toml", "w") as f:
        f.write(contents)

    # Open src/canonada/_version.py for writing
    with open("src/canonada/_version.py", "r") as f:
        contents = f.read()
        contents = contents.replace(f'__version__ = "{version}"', f'__version__ = "{new_version}"')
    with open("src/canonada/_version.py", "w") as f:
        f.write(contents)
    
    print(f"Version {version} bumped up to {new_version}")

