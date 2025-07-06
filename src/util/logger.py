import logging
import sys

from src.util.config import Config


def setup_logger(name):
    # Creo un logger con il nome che mi serve
    logger = logging.getLogger(name)
    logger.setLevel(Config.LOG_LEVEL)

    # Imposto il formato del log (data, nome, livello, messaggio)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Mando i log sull'output standard (es. terminale)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    # Aggiungo l'handler al logger
    logger.addHandler(handler)

    # Restituisco il logger pronto da usare
    return logger
