import pytest
from unittest.mock import patch, MagicMock
from src.service.tsc_config_client import get_tsc_configuration

# Valid configuration data for mocks
VALID_CONFIG_DATA = {
    "report_type": "daily_summary",
    "frequency": "daily",
    "fileType": "csv",
    "fileName": "report.csv",
    "fileStructure": [{"name": "col1"}],
    "job": "some_job_name",
    "packages": [],
    "customConfiguration": {}
}

# Common patches for tests
@patch('src.service.tsc_config_client.time.sleep')
@patch('src.service.tsc_config_client.requests.get')
@patch('src.service.tsc_config_client.logger')
def test_get_config_success_first_try(mock_logger, mock_get, mock_sleep):
    """Tests the success of the API call on the first attempt."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = VALID_CONFIG_DATA
    mock_get.return_value = mock_response

    config = get_tsc_configuration(tsc_id="t1", customer_id="c1")

    assert config is not None
    assert config["report_type"] == "daily_summary"
    mock_get.assert_called_once()
    mock_sleep.assert_not_called()
    mock_logger.warning.assert_not_called()

@patch('src.service.tsc_config_client.time.sleep')
@patch('src.service.tsc_config_client.requests.get')
@patch('src.service.tsc_config_client.logger')
def test_get_config_success_on_retry(mock_logger, mock_get, mock_sleep):
    """Tests the success of the API call after a failed attempt."""
    mock_success_response = MagicMock()
    mock_success_response.status_code = 200
    mock_success_response.json.return_value = VALID_CONFIG_DATA
    
    # Simulate a failure followed by a success
    mock_get.side_effect = [requests.exceptions.Timeout("Connection timed out"), mock_success_response]

    config = get_tsc_configuration(tsc_id="t1", customer_id="c1", max_retries=2)

    assert config is not None
    assert config["frequency"] == "daily"
    assert mock_get.call_count == 2
    mock_sleep.assert_called_once()
    mock_logger.warning.assert_called_once()

@patch('src.service.tsc_config_client.time.sleep')
@patch('src.service.tsc_config_client.requests.get')
@patch('src.service.tsc_config_client.logger')
def test_get_config_all_retries_fail(mock_logger, mock_get, mock_sleep):
    """Tests complete failure after exhausting all retries."""
    # Simulate continuous failures
    mock_get.side_effect = requests.exceptions.RequestException("Permanent error")

    config = get_tsc_configuration(tsc_id="t1", customer_id="c1", max_retries=3)

    assert config is None
    assert mock_get.call_count == 3
    assert mock_sleep.call_count == 2 # Sleeps between attempts 1-2 and 2-3
    mock_logger.error.assert_called_with("All REST attempts failed.")

@patch('src.service.tsc_config_client.time.sleep')
@patch('src.service.tsc_config_client.requests.get')
@patch('src.service.tsc_config_client.logger')
def test_get_config_validation_fails(mock_logger, mock_get, mock_sleep):
    """Tests the case where the API response is valid but the data is incomplete."""
    invalid_data = VALID_CONFIG_DATA.copy()
    del invalid_data["fileName"] # Remove a required field

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = invalid_data
    mock_get.return_value = mock_response

    config = get_tsc_configuration(tsc_id="t1", customer_id="c1")

    assert config is None
    mock_get.assert_called_once()
    mock_logger.error.assert_any_call("Missing required configuration fields: ['fileName']")
    mock_logger.error.assert_any_call("Invalid configuration received from REST service.")

# Add requests dependency for side_effect
import requests