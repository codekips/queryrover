import os
from ..log.logging import logger

def convert_to_absolute_if_needed(path:str) -> str:
    """Check if the provided path is relative and convert to absolute if needed."""
    if not os.path.isabs(path):
        # The path is relative, so convert it to an absolute path
        absolute_path = os.path.abspath(path)
        logger.info(f"Relative path '{path}' converted to absolute path: {absolute_path}")
        return absolute_path
    else:
        # The path is already absolute, so just return it
        print(f"Path '{path}' is already absolute.")
        return path
