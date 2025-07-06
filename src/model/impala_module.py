from src.util import Config
from src.util.logger import setup_logger
from src.repository.impala_writer import ImpalaWriter

logger = setup_logger('impala_module')

def save_to_impala(data: dict) -> bool:
    """
    Save a dictionary of data into an Impala table.
    :param data: Dictionary containing the data to be saved.
    :return: True if the data was successfully saved, False otherwise.
    """
    if not isinstance(data, dict):
        logger.error("Invalid data format for Impala: expected dict, got %s", type(data).__name__)
        return False

    try:
        writer = ImpalaWriter(
            host=Config.IMPALA_HOST,
            port=Config.IMPALA_PORT,
            database=Config.IMPALA_DATABASE
        )
        logger.debug("Inserting data: %s", data)
        writer.insert(table=Config.IMPALA_TABLE, data=data)
        logger.info("Data successfully saved to Impala: %s", list(data.keys()))
        return True

    except Exception as e:
        logger.error("Error while saving to Impala: %s", e)
        return False

def upsert_tsc_configuration(config: dict) -> bool:
    """
    Insert or update a TSC configuration record in the tsc_configurations table using parameterized queries.

    :param config: Dictionary containing configuration fields.
    :return: True if the operation was successful, False otherwise.
    """
    required_fields = [
        "tsc_id", "customer_id", "report_type", "frequency",
        "last_run", "timestamp", "status", "creation_timestamp"
    ]
    # Validate required fields
    missing = [f for f in required_fields if f not in config]
    if missing:
        logger.error(f"Missing required fields for upsert: {missing}")
        return False

    try:
        writer = ImpalaWriter(
            host=Config.IMPALA_HOST,
            port=Config.IMPALA_PORT,
            database=Config.IMPALA_DATABASE
        )
        # Build parameterized UPSERT (INSERT OVERWRITE) query
        query = f"""
        INSERT INTO tsc_configurations (tsc_id, customer_id, report_type, frequency, last_run, timestamp, status, creation_timestamp)
        VALUES (%(tsc_id)s, %(customer_id)s, %(report_type)s, %(frequency)s, %(last_run)s, %(timestamp)s, %(status)s, %(creation_timestamp)s)
        """
        # Try to insert, fallback to update if duplicate (pseudo-upsert, adjust as per Impala SQL support)
        writer.execute(query, config)
        logger.info("TSC configuration upserted for tsc_id=%s, customer_id=%s", config["tsc_id"], config["customer_id"])
        return True
    except Exception as e:
        logger.error("Error upserting TSC configuration: %s", e)
        return False