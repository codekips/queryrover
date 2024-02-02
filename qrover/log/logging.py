import logging
import sys
from typing import TextIO

logger = logging.getLogger("qrover")
logger.addHandler(logging.NullHandler())

def enable(level:int=logging.INFO, stream:TextIO=sys.stdout):
    handler = logging.StreamHandler(stream)
    logger.addHandler(handler)
    logger.setLevel(level)
