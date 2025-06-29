import json
import xml.etree.ElementTree as ET
import base64
import magic
from src.utils import config
from src.utils.logger import setup_logger

# Imposto il logger per questo modulo
logger = setup_logger('parser')


def parse_message(value, headers):
    """Capisco che tipo di dato ho e lo interpreto di conseguenza"""
    content_type = None

    # Cerco il Content-Type nei header
    for header in headers:
        if header[0] == "Content-Type":
            content_type = header[1].decode()
            break

    # Se non lo trovo, provo a capire il tipo da solo
    if not content_type:
        mime = magic.Magic(mime=True)
        content_type = mime.from_buffer(value)

    try:
        # Se è JSON, lo decodifico direttamente
        if content_type == "application/json":
            return json.loads(value.decode('utf-8'))

        # Se è XML, estraggo i tag e i valori
        elif content_type == "application/xml":
            root = ET.fromstring(value.decode('utf-8'))
            return {elem.tag: elem.text for elem in root.iter() if elem.text}

        # Se è un PDF, lo trasformo in base64
        elif content_type == "application/pdf":
            return {
                "content": base64.b64encode(value).decode('utf-8'),
                "metadata": {"type": "pdf"}
            }

        # Se è binario generico, lo codifico in base64
        elif content_type == "application/octet-stream":
            return {"binary_data": base64.b64encode(value).decode('utf-8')}

        else:
            # Se non so che tipo è, provo a decodificarlo come testo
            try:
                return value.decode('utf-8')
            except:
                # Altrimenti lo restituisco grezzo come stringa
                return {"raw_data": str(value)}

    except Exception as e:
        # Loggo l’errore e restituisco i dati grezzi
        logger.error(f"Parsing error: {e}")
        return {"error": str(e), "raw_data": str(value)}
