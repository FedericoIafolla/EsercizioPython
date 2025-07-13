import logging
import pytest
from unittest.mock import patch, MagicMock
from src.util.logger import setup_logger

# Patch Config and logging module components
@patch('src.util.logger.Config')
@patch('src.util.logger.logging.getLogger')
@patch('src.util.logger.logging.StreamHandler')
@patch('src.util.logger.logging.Formatter')
def test_setup_logger_default_level(mock_formatter, mock_handler, mock_get_logger, mock_config):
    """
    Tests that the logger is configured with the default INFO level.
    """
    # Setup: Config.LOG_LEVEL not set or invalid
    mock_config.LOG_LEVEL = "INVALID"

    mock_logger_instance = MagicMock(spec=logging.Logger)
    mock_logger_instance.handlers = [] # Initialize handlers list
    mock_get_logger.return_value = mock_logger_instance

    logger_name = "test_default"
    logger = setup_logger(logger_name)

    # Assertions
    mock_get_logger.assert_called_once_with(logger_name)
    mock_logger_instance.setLevel.assert_called_once_with(logging.INFO)
    mock_logger_instance.addHandler.assert_called_once()
    mock_handler.assert_called_once()
    mock_formatter.assert_called_once_with(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    mock_logger_instance.addHandler.call_args[0][0].setFormatter.assert_called_once_with(
        mock_formatter.return_value
    )
    assert logger == mock_logger_instance

@patch('src.util.logger.Config')
@patch('src.util.logger.logging.getLogger')
@patch('src.util.logger.logging.StreamHandler')
@patch('src.util.logger.logging.Formatter')
def test_setup_logger_custom_level(mock_formatter, mock_handler, mock_get_logger, mock_config):
    """
    Tests that the logger is configured with a custom level.
    """
    # Setup: Config.LOG_LEVEL set to DEBUG
    mock_config.LOG_LEVEL = "DEBUG"

    mock_logger_instance = MagicMock(spec=logging.Logger)
    mock_logger_instance.handlers = [] # Initialize handlers list
    mock_get_logger.return_value = mock_logger_instance

    logger_name = "test_custom"
    logger = setup_logger(logger_name)

    # Assertions
    mock_get_logger.assert_called_once_with(logger_name)
    mock_logger_instance.setLevel.assert_called_once_with(logging.DEBUG)
    mock_logger_instance.addHandler.assert_called_once()
    assert logger == mock_logger_instance

@patch('src.util.logger.Config')
@patch('src.util.logger.logging.getLogger')
@patch('src.util.logger.logging.StreamHandler')
@patch('src.util.logger.logging.Formatter')
def test_setup_logger_existing_handlers(mock_formatter, mock_handler, mock_get_logger, mock_config):
    """
    Tests that duplicate handlers are not added if the logger already has them.
    """
    mock_logger_instance = MagicMock(spec=logging.Logger)
    # Simulate a logger that already has a handler
    mock_logger_instance.handlers = [MagicMock()]
    mock_get_logger.return_value = mock_logger_instance

    # Ensure LOG_LEVEL is a string for getattr
    mock_config.LOG_LEVEL = "INFO"

    logger_name = "test_existing"
    setup_logger(logger_name)

    # Assertions: addHandler should not be called
    mock_logger_instance.addHandler.assert_not_called()