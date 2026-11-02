import json
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "sales-events"


def create_topic():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    futures = admin.create_topics([
        NewTopic(TOPIC, num_partitions=3, replication_factor=1)
    ])
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic créé : {topic}")
        except KafkaException as exc:
            # Le topic existe peut-être déjà : on ne bloque pas la démonstration.
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                print(f"Topic déjà existant : {topic}")
            else:
                raise


def delivery_report(err, msg):
    if err is not None:
        print(f"Échec de livraison : {err}")
    else:
        print(
            f"Message livré : topic={msg.topic()}, "
            f"partition={msg.partition()}, offset={msg.offset()}"
        )


def main():
    event_path = Path(__file__).with_name("event.json")
    with event_path.open("r", encoding="utf-8") as file:
        event = json.load(file)

    create_topic()

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
    })

    producer.produce(
        TOPIC,
        key=event["store_id"],
        value=json.dumps(event, ensure_ascii=False),
        callback=delivery_report,
    )

    remaining = producer.flush(10)
    if remaining:
        raise RuntimeError(f"{remaining} message(s) non livré(s)")

    print("Événement JSON envoyé.")


if __name__ == "__main__":
    main()
