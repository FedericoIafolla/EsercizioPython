from confluent_kafka import Consumer, KafkaError
from src.services import parser, job_builder
from src.models import impala_module, postgres_module
from src.utils import Config
from src.utils.logger import setup_logger

# Imposto il logger per questo modulo
logger = setup_logger('orchestrator')


def start_consumer():
    """Avvio il consumer Kafka e gestisco i messaggi in arrivo"""
    # Imposto la configurazione del consumer
    conf = {
        'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': Config.CONSUMER_GROUP,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'max.poll.interval.ms': 300000
    }

    # Creo il consumer e lo sottoscrivo al topic
    consumer = Consumer(conf)
    consumer.subscribe([Config.ORCHESTRATOR_TOPIC])

    logger.info("Orchestrator avviato. In attesa di messaggi...")

    try:
        while True:
            # Leggo un messaggio dal topic
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            # Gestisco eventuali errori del messaggio
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Errore consumer: {msg.error()}")
                continue

            try:
                # Estraggo e interpreto il contenuto del messaggio
                raw_value = msg.value()
                payload = parser.parse_message(raw_value, msg.headers())
                logger.info(f"Payload ricevuto: {payload}")

                # Creo il job e lo invio all’orchestratore
                job = job_builder.create_job(payload)
                job_builder.send_to_job_orchestrator(job)

                # Salvo i dati su Impala se il payload è valido
                if isinstance(payload, dict):
                    impala_module.save_to_impala(payload)
                    postgres_module.save_to_postgres(raw_value)
                else:
                    logger.warning("Payload non-dict saltato per Impala")

                # Confermo l’elaborazione del messaggio
                consumer.commit(msg)

            except Exception as e:
                logger.error(f"Errore durante l'elaborazione: {e}")
                # Qui potrei mandare il messaggio a una dead-letter queue

    except KeyboardInterrupt:
        logger.info("Interruzione ricevuta. Chiudo...")
    finally:
        # Chiudo il consumer e termino il processo
        consumer.close()


if __name__ == '__main__':
    start_consumer()
