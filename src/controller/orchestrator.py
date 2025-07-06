from src.service import cronjob_builder
from src.model import impala_module
from src.util.logger import setup_logger
from src.service.kafka_consumer import create_consumer, poll_message
from src.service import parser
from src.service import tsc_config_client
import datetime

logger = setup_logger('orchestrator')

def handle_create(payload: dict):
    """
    Handle the CREATE operation: fetch TSC configuration, persist configuration, create the job, and save to Impala.
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
    config_record = {
        "tsc_id": payload["tscId"],
        "customer_id": payload["customerId"],
        "report_type": tsc_config.get("report_type"),
        "frequency": tsc_config.get("frequency"),
        "last_run": None,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "status": "ACTIVE",
        "creation_timestamp": datetime.datetime.utcnow().isoformat()
    }
    impala_module.upsert_tsc_configuration(config_record)

    job = cronjob_builder.create_job(payload)
    cronjob_builder.send_to_job_orchestrator(job)

    if isinstance(payload, dict):
        impala_module.save_to_impala(payload)
    else:
        logger.warning("Non-dict payload skipped for Impala")

def handle_update(payload: dict):
    """
    Handle the UPDATE operation: fetch TSC configuration, persist configuration, create the job, and update Impala.
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
    config_record = {
        "tsc_id": payload["tscId"],
        "customer_id": payload["customerId"],
        "report_type": tsc_config.get("report_type"),
        "frequency": tsc_config.get("frequency"),
        "last_run": None,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "status": "UPDATED",
        "creation_timestamp": datetime.datetime.utcnow().isoformat()
    }
    impala_module.upsert_tsc_configuration(config_record)

    job = cronjob_builder.create_job(payload)
    cronjob_builder.send_to_job_orchestrator(job)

    if isinstance(payload, dict):
        impala_module.save_to_impala(payload)
    else:
        logger.warning("Non-dict payload skipped for Impala")

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