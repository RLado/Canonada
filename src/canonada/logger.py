import tomllib
import logging

# Read canonada.toml
config: dict
try:
    with open("canonada.toml", "rb") as f:
        config = tomllib.load(f)
except FileNotFoundError:
    config = {
        'logging': {
            'level': 'INFO'
        }
    }

# Create a logger
logger = logging.getLogger('canonada')
logger.setLevel(config['logging']['level'])
logging.basicConfig(format="%(asctime)s - %(name)s: [%(levelname)s]: %(message)s")


