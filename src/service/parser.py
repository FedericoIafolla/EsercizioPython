import json
from src.util.logger import setup_logger

# Set up the logger for this module
logger = setup_logger('parser')

# List of required fields that must be present in the message
REQUIRED_FIELDS = ["operation", "customerId", "tscId"]

def parse_message(raw_value, headers=None):
    """
    Parse the JSON message and validate the required fields.

    :param raw_value: The raw message value, as bytes or string.
    :param headers: Optional headers (not used in this function).
    :return: The parsed payload as a dictionary.
    :raises ValueError: If required fields are missing.
    :raises Exception: If parsing fails.
    """
    try:
        # Decode bytes to string if necessary and parse JSON
        payload = json.loads(raw_value.decode("utf-8") if isinstance(raw_value, bytes) else raw_value)
        # Check for missing required fields
        missing = [field for field in REQUIRED_FIELDS if field not in payload]
        if missing:
            logger.error(f"Message missing required fields: {missing}")
            raise ValueError(f"Missing required fields: {missing}")
        return payload
    except Exception as e:
        # Log and re-raise any parsing error
        logger.error(f"Error parsing message: {e}")
        raise