import pytest
from unittest.mock import patch, MagicMock, ANY
from src.controller import orchestrator

# Fixture for a base payload
@pytest.fixture
def base_payload():
    return {"operation": "CREATE", "customerId": "cust1", "tscId": "tsc1"}

# Fixture for a mocked Kafka message
@pytest.fixture
def mock_kafka_message(base_payload):
    msg = MagicMock()
    msg.value.return_value = str(base_payload).encode('utf-8')
    msg.headers.return_value = []
    msg.consumer = MagicMock() # Add a mocked consumer to the message
    return msg

# --- Tests for process_message --- #

@patch('src.controller.orchestrator.parser')
@patch('src.controller.orchestrator.OPERATION_HANDLERS')
def test_process_message_success(mock_handlers, mock_parser, mock_kafka_message, base_payload):
    """Tests the happy path of process_message with a valid operation."""
    mock_parser.parse_message.return_value = base_payload
    mock_handler = MagicMock()
    mock_handlers.get.return_value = mock_handler

    orchestrator.process_message(mock_kafka_message)

    mock_parser.parse_message.assert_called_once_with(mock_kafka_message.value(), mock_kafka_message.headers())
    mock_handlers.get.assert_called_once_with("CREATE")
    mock_handler.assert_called_once_with(base_payload)
    mock_kafka_message.consumer.commit.assert_called_once_with(mock_kafka_message)

@patch('src.controller.orchestrator.parser')
@patch('src.controller.orchestrator.logger')
def test_process_message_unsupported_operation(mock_logger, mock_parser, mock_kafka_message):
    """Tests that an unsupported operation is logged as an error."""
    payload = {"operation": "DELETE", "customerId": "cust1", "tscId": "tsc1"}
    mock_parser.parse_message.return_value = payload

    orchestrator.process_message(mock_kafka_message)

    mock_logger.error.assert_called_with("Unsupported operation: DELETE")
    mock_kafka_message.consumer.commit.assert_called_once_with(mock_kafka_message)

@patch('src.controller.orchestrator.parser')
@patch('src.controller.orchestrator.logger')
def test_process_message_parsing_error(mock_logger, mock_parser, mock_kafka_message):
    """Tests that a parsing error prevents commit."""
    error = ValueError("Invalid JSON")
    mock_parser.parse_message.side_effect = error

    orchestrator.process_message(mock_kafka_message)

    mock_logger.error.assert_called_with(f"Error during processing: {error}")
    mock_kafka_message.consumer.commit.assert_not_called()

# --- Test for _process_job_operation (indirectly tested via handle_create) --- #

@patch('src.controller.orchestrator.tsc_config_client')
@patch('src.controller.orchestrator.impala_module')
@patch('src.controller.orchestrator.cronjob_builder')
@patch('src.controller.orchestrator.cronjob_applier')
@patch('src.controller.orchestrator.logger')
@patch('src.controller.orchestrator.datetime')
def test_handle_create_success(mock_dt, mock_logger, mock_applier, mock_builder, mock_impala, mock_tsc_client, base_payload):
    """Tests the entire logical flow of handle_create on success."""
    # Setup mocks
    mock_tsc_client.get_tsc_configuration.return_value = {"frequency": "daily", "report_type": "full"}
    mock_builder.generate_cronjob_manifest.return_value = "yaml_manifest"
    mock_applier.apply_cronjob_manifest.return_value = True
    mock_dt.now.return_value.isoformat.return_value = "fake-iso-timestamp"

    # Execution
    orchestrator.handle_create(base_payload)

    # Assertions
    mock_tsc_client.get_tsc_configuration.assert_called_once_with(tsc_id='tsc1', customer_id='cust1')
    mock_impala.upsert_tsc_configuration.assert_called_once()
    mock_builder.generate_cronjob_manifest.assert_called_once()
    mock_applier.apply_cronjob_manifest.assert_called_once_with("yaml_manifest")
    mock_impala.save_to_impala.assert_called_once()
    mock_logger.info.assert_any_call("Successfully applied CronJob for tscId: tsc1")

# --- Test for start_consumer --- #

@patch('src.controller.orchestrator.create_consumer')
@patch('src.controller.orchestrator.poll_message')
@patch('src.controller.orchestrator.process_message')
def test_start_consumer_loop_and_exit(mock_process, mock_poll, mock_create):
    """Tests that the consumer loop processes a message and closes correctly."""
    mock_consumer = MagicMock()
    mock_create.return_value = mock_consumer
    mock_msg = MagicMock()
    # Simulate: 1. a message, 2. an interruption to exit the loop
    mock_poll.side_effect = [mock_msg, KeyboardInterrupt]

    # We don't use pytest.raises because the exception is handled within the function.
    orchestrator.start_consumer()

    # Assertions
    mock_create.assert_called_once()
    assert mock_poll.call_count == 2
    mock_process.assert_called_once_with(mock_msg)
    # Verify that the consumer attribute is added to the message
    assert mock_msg.consumer == mock_consumer
    mock_consumer.close.assert_called_once()