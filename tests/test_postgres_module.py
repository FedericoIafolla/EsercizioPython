import pytest
import psycopg2
from unittest.mock import patch, MagicMock
from src.model.postgres_module import save_to_postgres

# Patch for the Config object and for the logger within the module under test
@patch('src.model.postgres_module.Config')
@patch('src.model.postgres_module.logger')
@patch('src.model.postgres_module.psycopg2.connect')
def test_save_to_postgres_success(mock_connect, mock_logger, mock_config):
    """
    Tests saving to Postgres on success.
    """
    # 1. Mock configuration
    # Mock connection and cursor
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    # Mock configuration parameters
    mock_config.POSTGRES_HOST = 'test-host'
    mock_config.POSTGRES_PORT = 5432
    mock_config.POSTGRES_DB = 'test-db'
    mock_config.POSTGRES_USER = 'test-user'
    mock_config.POSTGRES_PASSWORD = 'test-password'

    # 2. Execute the function
    raw_data_json = '{"key": "value"}'
    result = save_to_postgres(raw_data_json)

    # 3. Assertions
    # Verify that the connection was called with data from mock_config
    mock_connect.assert_called_once_with(
        host='test-host',
        port=5432,
        dbname='test-db',
        user='test-user',
        password='test-password'
    )
    
    # Verify that the query was executed correctly
    mock_cursor.execute.assert_called_once_with(
        "INSERT INTO processed_data (payload) VALUES (%s)",
        [raw_data_json]
    )
    
    # Verify that the transaction was committed and resources closed
    mock_conn.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()
    
    # Verify logging and return value
    mock_logger.info.assert_called_once_with("Dati RAW salvati su Postgres")
    assert result is True

@patch('src.model.postgres_module.Config')
@patch('src.model.postgres_module.logger')
@patch('src.model.postgres_module.psycopg2.connect')
def test_save_to_postgres_db_error(mock_connect, mock_logger, mock_config):
    """
    Tests error handling during saving to Postgres.
    """
    # 1. Mock configuration
    # Simulate an error during connection
    db_error = psycopg2.OperationalError("Connection failed")
    mock_connect.side_effect = db_error

    # 2. Execute the function
    result = save_to_postgres('some data')

    # 3. Assertions
    # Verify that the error was logged
    mock_logger.error.assert_called_once_with(f"Errore Postgres: {db_error}")
    
    # Verify that the function returns False
    assert result is False