import logging
import tomllib

# Create a logger only for this module
_logger = logging.getLogger("canonada")
logging.basicConfig(format="%(asctime)s - %(name)s: [%(levelname)s]: %(message)s")

# Read canonada.toml
config: dict
try:
    with open("canonada.toml", "rb") as f:
        config = tomllib.load(f)
except FileNotFoundError: # This case will happen when no project is created, default values will be used
    _logger.warning("Canonada configuration file not found")
    config = {}
except tomllib.TOMLDecodeError as e:
    raise ValueError(f"Can not decode Canonada configuration file: {e}")