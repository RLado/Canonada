import logging
from ._config import config


# Create a logger
logger = logging.getLogger("canonada")
logger.setLevel(config.get("logging",{}).get("level", "INFO"))
logging.basicConfig(format="%(asctime)s - %(name)s: [%(levelname)s]: %(message)s")
