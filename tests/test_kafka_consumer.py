import pytest
from unittest.mock import patch, MagicMock
from src.service.kafka_consumer import create_consumer, poll_message

# Patches for Config and Consumer in the kafka_consumer module
@patch('src.service.kafka_consumer.Config')
@patch('src.service.kafka_consumer.Consumer')
@patch('src.service.kafka_consumer.logger')
def test_create_consumer(mock_logger, mock_consumer_class, mock_config):
    """Tests the creation and configuration of the Kafka Consumer."""
    # 1. Mock configuration
    mock_config.KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
    mock_config.CONSUMER_GROUP = 'test-group'
    mock_config.ORCHESTRATOR_TOPIC = 'test-topic'
    
    mock_consumer_instance = MagicMock()
    mock_consumer_class.return_value = mock_consumer_instance

    # 2. Execute the function
    consumer = create_consumer()

    # 3. Assertions
    expected_conf = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'test-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'max.poll.interval.ms': 300000
    }
    mock_consumer_class.assert_called_once_with(expected_conf)
    mock_consumer_instance.subscribe.assert_called_once_with(['test-topic'])
    assert consumer == mock_consumer_instance
    mock_logger.info.assert_called_with("Kafka consumer created and subscribed to topic.")

@patch('src.service.kafka_consumer.logger')
def test_poll_message_success(mock_logger):
    """Tests polling a message successfully."""
    mock_consumer = MagicMock()
    mock_msg = MagicMock()
    mock_msg.error.return_value = None  # No error
    mock_consumer.poll.return_value = mock_msg

    result = poll_message(mock_consumer)

    mock_consumer.poll.assert_called_once_with(timeout=1.0)
    assert result == mock_msg
    mock_logger.error.assert_not_called()

@patch('src.service.kafka_consumer.logger')
def test_poll_message_timeout(mock_logger):
    """Tests the case where polling times out (no message)."""
    mock_consumer = MagicMock()
    mock_consumer.poll.return_value = None

    result = poll_message(mock_consumer)

    assert result is None
    mock_logger.error.assert_not_called()

@patch('src.service.kafka_consumer.logger')
def test_poll_message_kafka_error(mock_logger):
    """Tests handling of a generic Kafka error."""
    mock_consumer = MagicMock()
    mock_msg = MagicMock()
    mock_error = MagicMock()
    mock_error.name.return_value = 'KAFKA_ERROR_FATAL'
    mock_msg.error.return_value = mock_error
    mock_consumer.poll.return_value = mock_msg

    result = poll_message(mock_consumer)

    assert result is None
    mock_logger.error.assert_called_once_with(f"Consumer error: {mock_error}")

@patch('src.service.kafka_consumer.logger')
def test_poll_message_partition_eof(mock_logger):
    """Tests handling of the non-error 'End of Partition' event."""
    mock_consumer = MagicMock()
    mock_msg = MagicMock()
    mock_error = MagicMock()
    # The error name in confluent-kafka is _PARTITION_EOF, but the check is on 'PARTITION_EOF'
    mock_error.name.return_value = 'PARTITION_EOF'
    mock_msg.error.return_value = mock_error
    mock_consumer.poll.return_value = mock_msg

    result = poll_message(mock_consumer)

    assert result is None
    mock_logger.debug.assert_called_once()
    mock_logger.error.assert_not_called()