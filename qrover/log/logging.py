import logging
import sys

import qrover

logger = logging.getLogger(qrover.__name__)
logger.addHandler(logging.NullHandler())

def enable(level=logging.INFO, stream=sys.stdout):
    handler = logging.StreamHandler(stream)
    logger.addHandler(handler)
    logger.setLevel(level)
