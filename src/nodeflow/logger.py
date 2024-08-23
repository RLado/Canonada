import tomllib
import logging

# Read nodeflow.toml
config: dict
try:
    with open("nodeflow.toml", "rb") as f:
        config = tomllib.load(f)
except FileNotFoundError:
    config = {
        'logging': {
            'level': 'INFO'
        }
    }

# Create a logger
logger = logging.getLogger('nodeflow')
logger.setLevel(config['logging']['level'])
logging.basicConfig(format="%(asctime)s - %(name)s: [%(levelname)s]: %(message)s")


