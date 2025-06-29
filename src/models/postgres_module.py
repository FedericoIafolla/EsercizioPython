import psycopg2
import json
from src.utils import Config
from src.utils.logger import setup_logger

logger = setup_logger('postgres_module')


def save_to_postgres(raw_data):
    """
    Salva il payload JSON grezzo nella colonna 'payload' della tabella
    """
    try:
        conn = psycopg2.connect(
            host=Config.POSTGRES_HOST,
            port=Config.POSTGRES_PORT,
            dbname=Config.POSTGRES_DB,
            user=Config.POSTGRES_USER,
            password=Config.POSTGRES_PASSWORD
        )
        cur = conn.cursor()

        # MODIFICA QUI: inserisci i byte grezzi direttamente
        cur.execute(
            "INSERT INTO processed_data (payload) VALUES (%s)",
            [raw_data]  # usa i dati grezzi invece di json.dumps
        )

        conn.commit()
        cur.close()
        conn.close()
        logger.info("Dati RAW salvati su Postgres")
        return True
    except Exception as e:
        logger.error(f"Errore Postgres: {e}")
        return False
