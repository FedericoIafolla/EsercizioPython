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
        # Mi collego al database Postgres usando i parametri di configurazione
        conn = psycopg2.connect(
            host=Config.POSTGRES_HOST,
            port=Config.POSTGRES_PORT,
            dbname=Config.POSTGRES_DB,
            user=Config.POSTGRES_USER,
            password=Config.POSTGRES_PASSWORD
        )
        cur = conn.cursor()

        # Inserisco direttamente i dati grezzi (byte o stringa JSON) nella colonna 'payload'
        cur.execute(
            "INSERT INTO processed_data (payload) VALUES (%s)",
            [raw_data]
        )

        # Confermo la scrittura sul database
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Dati RAW salvati su Postgres")
        return True
    except Exception as e:
        # Se qualcosa va storto, scrivo l'errore nei log e restituisco False
        logger.error(f"Errore Postgres: {e}")
        return False