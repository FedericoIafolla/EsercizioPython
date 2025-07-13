from src.service import cronjob_builder
from src.model import impala_module
from src.util.logger import setup_logger
from src.service.kafka_consumer import create_consumer, poll_message
from src.service import parser
from src.service import tsc_config_client
from datetime import datetime, timezone
# Removed: from confluent_kafka import Producer
from src.service import cronjob_applier  # New import

logger = setup_logger('orchestrator')


# Removed: PRODUCER_CONF and JOB_TOPIC

def _process_job_operation(payload: dict, status: str):
    """
    Handles the common logic for CREATE and UPDATE operations.
    """
    # Retrieve TSC configuration using both customerId and tscId
    tsc_config = tsc_config_client.get_tsc_configuration(
        tsc_id=payload["tscId"],
        customer_id=payload["customerId"]
    )
    if tsc_config is None:
        logger.error("TSC configuration not available")
        return

    payload.update(tsc_config)

    # Prepare and upsert configuration record in Impala
    now_iso = datetime.now(timezone.utc).isoformat()
    config_record = {
        "tsc_id": payload["tscId"],
        "customer_id": payload["customerId"],
        "report_type": tsc_config.get("report_type"),
        "frequency": tsc_config.get("frequency"),
        "last_run": None,
        "timestamp": now_iso,
        "status": status,  # Use the passed status
        "creation_timestamp": now_iso,
    }
    impala_module.upsert_tsc_configuration(config_record)

    job_manifest = cronjob_builder.generate_cronjob_manifest(payload)
    # Removed: producer = Producer(PRODUCER_CONF)
    # Removed: cronjob_builder.send_to_job_orchestrator(job, producer, JOB_TOPIC)

    # New: Apply the CronJob manifest to Kubernetes
    if cronjob_applier.apply_cronjob_manifest(job_manifest):
        logger.info(f"Successfully applied CronJob for tscId: {payload['tscId']}")
    else:
        logger.error(f"Failed to apply CronJob for tscId: {payload['tscId']}")

    if isinstance(payload, dict):
        impala_module.save_to_impala(payload)
    else:
        logger.warning("Non-dict payload skipped for Impala")


def handle_create(payload: dict):
    """
    Handle the CREATE operation: fetch TSC configuration, persist configuration, create the job, and save to Impala.
    """
    _process_job_operation(payload, "ACTIVE")


def handle_update(payload: dict):
    """
    Handle the UPDATE operation: fetch TSC configuration, persist configuration, create the job, and update Impala.
    """
    _process_job_operation(payload, "UPDATED")


# Supported operations map
OPERATION_HANDLERS = {
    "CREATE": handle_create,
    "UPDATE": handle_update,
}


def process_message(msg):
    """
    elaborate a message from Kafka
    """
    try:
        raw_value = msg.value()
        payload = parser.parse_message(raw_value, msg.headers())
        logger.debug(f"Received payload: {payload}")

        operation = payload.get("operation", "").upper()
        handler = OPERATION_HANDLERS.get(operation)
        if handler:
            handler(payload)
        else:
            logger.error(f"Unsupported operation: {operation}")

        msg.consumer.commit(msg)
        logger.info("Consumer committed for message: %s", msg.offset())
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        # TODO: implementing dead-letter queue logic


def start_consumer():
    """
    start the Kafka consumer and listen for messages
    """
    consumer = create_consumer()
    logger.info("Orchestrator started. Waiting for messages...")

    try:
        while True:
            msg = poll_message(consumer)
            if msg is None:
                continue
            # add the attribute consumer to the message for commit
            msg.consumer = consumer
            process_message(msg)
    except KeyboardInterrupt:
        logger.warning("Interrupt received. Closing...")
    finally:
        consumer.close()


if __name__ == '__main__':
    start_consumer()