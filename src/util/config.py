import os


class Config:
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9093')
    ORCHESTRATOR_TOPIC = os.getenv('ORCHESTRATOR_TOPIC', 'orchestrator_topic')
    JOB_ORCHESTRATOR_TOPIC = os.getenv('JOB_ORCHESTRATOR_TOPIC', 'job_orchestrator_topic')

    # Consumer
    CONSUMER_GROUP = os.getenv('CONSUMER_GROUP', 'orchestrator_group')

    # Postgres
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'imp2')
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', '1234')

    # Impala
    IMPALA_HOST = os.getenv('IMPALA_HOST', 'impala-host')
    IMPALA_PORT = int(os.getenv('IMPALA_PORT', 21050))
    IMPALA_DATABASE = os.getenv('IMPALA_DATABASE', 'default')
    IMPALA_TABLE = os.getenv('IMPALA_TABLE', 'processed_data')

    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')