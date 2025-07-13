import json
import pytest
from src.service.parser import parse_message, REQUIRED_FIELDS

# Example of valid JSON payload for tests
VALID_PAYLOAD = {
    "operation": "create",
    "customerId": "12345",
    "tscId": "abcdef"
}

def test_parse_message_success():
    """
    Tests the deserialization of a valid JSON message.
    """
    json_string = json.dumps(VALID_PAYLOAD)
    parsed = parse_message(json_string)
    assert parsed == VALID_PAYLOAD

def test_parse_message_bytes_input():
    """
    Tests deserialization when the input is in bytes format.
    """
    json_bytes = json.dumps(VALID_PAYLOAD).encode('utf-8')
    parsed = parse_message(json_bytes)
    assert parsed == VALID_PAYLOAD

def test_parse_message_missing_fields():
    """
    Tests that a ValueError exception is raised if required fields are missing.
    """
    # Create a payload missing the 'tscId' field
    payload_missing_field = VALID_PAYLOAD.copy()
    del payload_missing_field["tscId"]
    
    json_string = json.dumps(payload_missing_field)
    
    # Verify that the expected exception is raised
    with pytest.raises(ValueError) as excinfo:
        parse_message(json_string)
    
    # Verify that the error message contains the name of the missing field
    assert "Missing required fields" in str(excinfo.value)
    assert "'tscId'" in str(excinfo.value)

def test_parse_message_invalid_json():
    """
    Tests that a JSONDecodeError exception is raised for a malformed JSON.
    """
    invalid_json_string = '{"operation": "create", "customerId": "12345", "tscId": "abcdef"' # Missing a }
    
    # The parse_message function re-raises the original json.JSONDecodeError.
    # So we verify that exactly that type of exception is raised.
    with pytest.raises(json.JSONDecodeError):
        parse_message(invalid_json_string)