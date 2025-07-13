import pytest
from unittest.mock import patch, MagicMock
from src.model.impala_module import save_to_impala, upsert_tsc_configuration

# Common patches for Config and ImpalaWriter in the module under test
@patch('src.model.impala_module.Config')
@patch('src.model.impala_module.ImpalaWriter')
@patch('src.model.impala_module.logger')
def test_save_to_impala_success(mock_logger, mock_impala_writer, mock_config):
    """Tests saving to Impala on success."""
    # Setup
    mock_writer_instance = MagicMock()
    mock_impala_writer.return_value = mock_writer_instance
    mock_config.IMPALA_TABLE = 'test_table'
    data = {'col1': 'val1'}

    # Execution
    result = save_to_impala(data)

    # Assertions
    mock_impala_writer.assert_called_once()
    mock_writer_instance.insert.assert_called_once_with(table='test_table', data=data)
    mock_logger.info.assert_called_with("Data successfully saved to Impala: %s", list(data.keys()))
    assert result is True

@patch('src.model.impala_module.Config')
@patch('src.model.impala_module.ImpalaWriter')
@patch('src.model.impala_module.logger')
def test_save_to_impala_failure(mock_logger, mock_impala_writer, mock_config):
    """Tests error handling when saving to Impala."""
    # Setup
    db_error = Exception("Connection failed")
    mock_writer_instance = MagicMock()
    mock_writer_instance.insert.side_effect = db_error
    mock_impala_writer.return_value = mock_writer_instance

    # Execution
    result = save_to_impala({'col1': 'val1'})

    # Assertions
    mock_logger.error.assert_called_with("Error while saving to Impala: %s", db_error)
    assert result is False

# --- Test for upsert_tsc_configuration --- #

@pytest.fixture
def valid_upsert_config():
    return {
        "tsc_id": "t1", "customer_id": "c1", "report_type": "r1",
        "frequency": "daily", "last_run": None, "timestamp": "ts",
        "status": "ACTIVE", "creation_timestamp": "cts"
    }

@patch('src.model.impala_module.Config')
@patch('src.model.impala_module.ImpalaWriter')
@patch('src.model.impala_module.logger')
def test_upsert_success(mock_logger, mock_impala_writer, mock_config, valid_upsert_config):
    """Tests the upsert operation on success."""
    mock_writer_instance = MagicMock()
    mock_impala_writer.return_value = mock_writer_instance

    result = upsert_tsc_configuration(valid_upsert_config)

    mock_writer_instance.execute.assert_called_once()
    # Exact query assertion is fragile, so we focus on the call
    assert result is True
    mock_logger.info.assert_called_once()

@patch('src.model.impala_module.ImpalaWriter')
@patch('src.model.impala_module.logger')
def test_upsert_missing_fields(mock_logger, mock_impala_writer):
    """Tests that upsert fails if required fields are missing."""
    invalid_config = {"tsc_id": "t1"} # many fields are missing
    result = upsert_tsc_configuration(invalid_config)

    assert result is False
    mock_logger.error.assert_called_with(f"Missing required fields for upsert: {['customer_id', 'report_type', 'frequency', 'last_run', 'timestamp', 'status', 'creation_timestamp']}")
    mock_impala_writer.assert_not_called()