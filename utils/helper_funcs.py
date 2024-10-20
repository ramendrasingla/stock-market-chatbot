import logging
from datetime import timezone
from logging.handlers import RotatingFileHandler

def generate_id(input_data):
    """
    Generates a unique integer ID for the given input data.

    Args:
        input_data (str, int, or any hashable object): Input data to generate the ID from.

    Returns:
        int: A unique integer ID.
    """
    return abs(hash(input_data))

# Configure logging
def setup_logging(log_path = './data/logs/pipeline.log'):
    logger = logging.getLogger('stock_pipeline')
    logger.setLevel(logging.INFO)

    # Create a file handler for logging to a file
    file_handler = RotatingFileHandler(log_path, maxBytes=2000000, backupCount=5)
    file_handler.setLevel(logging.INFO)

    # Create a console handler for logging to the terminal
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create a logging format
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

# Convert a naive datetime to an aware datetime in UTC
def to_utc(dt):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)  # Make it aware with UTC timezone
    return dt