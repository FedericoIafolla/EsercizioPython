from confluent_kafka import Producer
import json
import uuid
from src.utils import Config
from src.utils.logger import setup_logger

# Imposto il logger per questo modulo
logger = setup_logger('job_builder')


def create_job(data):
    """Creo un job con ID unico e info base"""
    job_id = str(uuid.uuid4())
    return {
        "job_id": job_id,
        "status": "CREATED",
        "data": data,
        "metadata": {"source": "kafka_orchestrator"}
    }


def send_to_job_orchestrator(job_data):
    """Invio il job al topic Kafka dell'orchestratore"""
    # Configuro il producer Kafka
    conf = {'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS}
    producer = Producer(conf)

    try:
        # Mando il messaggio con chiave e payload JSON
        producer.produce(
            topic=Config.JOB_ORCHESTRATOR_TOPIC,
            key=job_data["job_id"],
            value=json.dumps(job_data).encode('utf-8'),
            headers=[("Content-Type", "application/json")]
        )
        # Forzo l'invio dei messaggi in coda
        producer.flush()
        logger.info(f"Job inviato all'orchestratore: {job_data['job_id']}")
        return True
    except Exception as e:
        # Loggo l'errore se l'invio fallisce
        logger.error(f"Errore invio job: {e}")
        return False
