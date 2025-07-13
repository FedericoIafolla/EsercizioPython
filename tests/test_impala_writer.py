import pytest
from unittest.mock import patch, MagicMock
from src.repository.impala_writer import ImpalaWriter

@pytest.fixture
def impala_writer_config():
    """Fixture to provide a standard configuration for ImpalaWriter."""
    return {
        "host": "test-host",
        "port": 12345,
        "database": "test_db"
    }

@patch('src.repository.impala_writer.ImpalaWriter.execute')
def test_insert_success(mock_execute, impala_writer_config):
    """
    Tests that the insert method constructs the correct query and calls execute.
    """
    writer = ImpalaWriter(**impala_writer_config)
    table_name = "my_test_table"
    data_to_insert = {"col1": "value1", "col2": 100}

    writer.insert(table_name, data_to_insert)

    expected_query = "INSERT INTO my_test_table (col1, col2) VALUES (%s, %s)"
    expected_values = list(data_to_insert.values())
    mock_execute.assert_called_once_with(expected_query, expected_values)

@patch('src.repository.impala_writer.ImpalaWriter.execute')
def test_insert_database_error(mock_execute, impala_writer_config):
    """
    Tests that a database exception during insert is propagated correctly.
    """
    db_error = Exception("Impala insert failed")
    mock_execute.side_effect = db_error

    writer = ImpalaWriter(**impala_writer_config)
    data_to_insert = {"col1": "value1"}

    with pytest.raises(Exception) as excinfo:
        writer.insert("any_table", data_to_insert)

    assert excinfo.value == db_error
    mock_execute.assert_called_once()