from confluent_kafka import Producer
import json
import time


# Funzione di callback per sapere se il messaggio è stato consegnato con successo o meno
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def send_json_message():
    # Configuro il producer Kafka (localhost:9092 nel mio caso)
    conf = {'bootstrap.servers': 'localhost:9092'}

    producer = Producer(conf)

    # Creo un messaggio JSON di esempio (dati utente fittizi)
    data = {
        "id": "12345",
        "type": "user",
        "attributes": {
            "name": "Mario Rossi",
            "email": "mario.rossi@example.com",
            "age": 35,
            "is_active": True
        },
        "metadata": {
            "created_at": "2023-10-15T12:30:45Z",
            "source": "web_form"
        }
    }

    try:
        # Invio il messaggio al topic Kafka 'orchestrator_topic'
        producer.produce(
            topic='orchestrator_topic',  # Deve combaciare con quello nel docker-compose/env
            key='test_key',  # Chiave arbitraria
            value=json.dumps(data).encode('utf-8'),  # Serializzo in JSON
            headers=[("Content-Type", "application/json")],  # Header utile per il parser
            callback=delivery_report  # Callback per loggare il risultato
        )
        producer.flush()  # Forzo l’invio di tutti i messaggi in coda
        print("JSON message sent successfully!")
    except Exception as e:
        print(f"Error sending message: {e}")


if __name__ == '__main__':
    send_json_message()
