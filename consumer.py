import json
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "sales-events"


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "sales-app",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([TOPIC])
    print(f"En attente d'un message sur {TOPIC}...")

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(f"Erreur Kafka : {message.error()}")
                continue

            try:
                event = json.loads(message.value().decode("utf-8"))
                print("\nÉvénement JSON reçu :")
                print(json.dumps(event, indent=2, ensure_ascii=False))

                # Le commit intervient seulement après le traitement réussi.
                consumer.commit(message=message, asynchronous=False)
                print(
                    f"Offset validé : partition={message.partition()}, "
                    f"offset={message.offset()}"
                )
                break
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                print(f"Message JSON invalide : {exc}")
                # Pas de commit : le message pourra être relu.
    except KeyboardInterrupt:
        print("Arrêt demandé.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
