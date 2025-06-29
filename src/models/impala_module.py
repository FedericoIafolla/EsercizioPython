from impala.dbapi import connect
from src.utils import Config
from src.utils.logger import setup_logger

# Imposto il logger per questo modulo
logger = setup_logger('impala_module')


def save_to_impala(data):
    """Salvo un dizionario di dati in una tabella Impala"""
    # Controllo che i dati siano nel formato corretto
    if not isinstance(data, dict):
        logger.error("Formato dati non valido per Impala")
        return False

    try:
        # Apro connessione a Impala usando le configurazioni
        conn = connect(
            host=Config.IMPALA_HOST,
            port=Config.IMPALA_PORT,
            database=Config.IMPALA_DATABASE
        )
        cursor = conn.cursor()

        # Preparo colonne e segnaposto per i valori
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        # Creo la query di inserimento
        query = f"""
            INSERT INTO BOH
            VALUES BOH2
        """

        # Eseguo l'insert passando i valori
        cursor.execute(query, list(data.values()))
        # Chiudo cursore e connessione
        cursor.close()
        conn.close()

        logger.info(f"Dati salvati su Impala: {list(data.keys())}")
        return True

    except Exception as e:
        # Segnalo l'errore se qualcosa va storto
        logger.error(f"Errore Impala: {e}")
        return False
