from confluent_kafka import Consumer
from src.util import Config
from src.util.logger import setup_logger

logger = setup_logger('kafka_consumer')

def create_consumer():
    """
    Create and return a configured Kafka Consumer object.

    :return: Configured Kafka Consumer instance.
    """
    conf = {
        'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': Config.CONSUMER_GROUP,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'max.poll.interval.ms': 300000
    }
    consumer = Consumer(conf)
    consumer.subscribe([Config.ORCHESTRATOR_TOPIC])
    logger.info("Kafka consumer created and subscribed to topic.")
    return consumer

def poll_message(consumer, timeout=1.0):
    """
    Poll a message from the Kafka consumer.

    :param consumer: The Kafka Consumer instance.
    :param timeout: Polling timeout in seconds.
    :return: The polled message, or None if no message is available or an error occurs.
    """
    msg = consumer.poll(timeout=timeout)
    if msg is None:
        return None

    err = msg.error()
    if err is not None:
        if err.name() == 'PARTITION_EOF':
            logger.debug(f"End of partition reached: {err}")
            return None
        logger.error(f"Consumer error: {msg.error()}")
        return None
    return msg