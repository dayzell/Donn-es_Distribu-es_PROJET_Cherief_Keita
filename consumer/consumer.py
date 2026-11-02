"""Consumer (Reference Loader du cahier des charges) : Kafka -> PostgreSQL.

Relit entièrement retail-products et retail-customers depuis le début, puis
fait un upsert dans les tables products et customers. Le chargement est
rejouable sans doublons : la clé est l'identifiant DummyJSON. Le consumer
s'arrête une fois la fin des topics atteinte (comportement de Job).
"""
import json
import logging
import os
import sys

import psycopg2
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaException, TopicPartition
from psycopg2.extras import execute_values

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s consumer %(message)s",
)
log = logging.getLogger()

# Pour chaque topic : colonnes attendues, clé primaire et requête d'upsert.
TABLES = {
    "retail-products": {
        "table": "products",
        "columns": ["product_id", "title", "category", "price", "stock", "brand"],
        "sql": """
            INSERT INTO products (product_id, title, category, price, stock, brand)
            VALUES %s
            ON CONFLICT (product_id) DO UPDATE SET
                title = EXCLUDED.title, category = EXCLUDED.category,
                price = EXCLUDED.price, stock = EXCLUDED.stock,
                brand = EXCLUDED.brand, updated_at = now()
        """,
    },
    "retail-customers": {
        "table": "customers",
        "columns": ["customer_id", "first_name", "last_name", "email", "city"],
        "sql": """
            INSERT INTO customers (customer_id, first_name, last_name, email, city)
            VALUES %s
            ON CONFLICT (customer_id) DO UPDATE SET
                first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name,
                email = EXCLUDED.email, city = EXCLUDED.city, updated_at = now()
        """,
    },
}


def connect_postgres():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def assign_from_beginning(consumer):
    """Assigne toutes les partitions au début et retourne les offsets de fin à atteindre."""
    metadata = consumer.list_topics(timeout=15)
    partitions, end_offsets = [], {}
    for topic in TABLES:
        if topic not in metadata.topics or metadata.topics[topic].error:
            raise KafkaException(f"topic {topic} introuvable")
        for partition in metadata.topics[topic].partitions:
            tp = TopicPartition(topic, partition, OFFSET_BEGINNING)
            _, high = consumer.get_watermark_offsets(tp, timeout=15)
            partitions.append(tp)
            if high > 0:
                end_offsets[(topic, partition)] = high
    consumer.assign(partitions)
    return end_offsets


def parse(message, columns):
    """Retourne le tuple de valeurs, ou None si le message est invalide."""
    try:
        record = json.loads(message.value().decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, AttributeError) as exc:
        log.warning("%s[%d]@%d : JSON invalide (%s)",
                    message.topic(), message.partition(), message.offset(), exc)
        return None
    if record.get(columns[0]) is None:
        log.warning("%s[%d]@%d : identifiant manquant",
                    message.topic(), message.partition(), message.offset())
        return None
    return tuple(record.get(column) for column in columns)


def flush(connection, batches, stats):
    """Écrit les lots en attente dans une transaction par topic."""
    for topic, rows in batches.items():
        if not rows:
            continue
        # Une même clé peut apparaître plusieurs fois (topic pas encore compacté) :
        # on garde la dernière version, sinon l'upsert échoue sur un lot.
        latest = list({row[0]: row for row in rows}.values())
        with connection.cursor() as cursor:
            execute_values(cursor, TABLES[topic]["sql"], latest)
        connection.commit()
        stats[topic]["upserted"] += len(latest)
        rows.clear()


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "reference-loader",
        "enable.auto.commit": False,  # relecture complète à chaque exécution
    })
    connection = connect_postgres()
    stats = {topic: {"read": 0, "invalid": 0, "upserted": 0} for topic in TABLES}
    batches = {topic: [] for topic in TABLES}

    try:
        remaining = assign_from_beginning(consumer)
        log.info("Lecture depuis le début : %s", {f"{t}[{p}]": o for (t, p), o in remaining.items()})

        while remaining:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())

            topic = message.topic()
            stats[topic]["read"] += 1
            row = parse(message, TABLES[topic]["columns"])
            if row is None:
                stats[topic]["invalid"] += 1
            else:
                batches[topic].append(row)
                if len(batches[topic]) >= BATCH_SIZE:
                    flush(connection, batches, stats)

            # Partition terminée quand on atteint l'offset de fin mesuré au démarrage.
            key = (topic, message.partition())
            if key in remaining and message.offset() + 1 >= remaining[key]:
                del remaining[key]

        flush(connection, batches, stats)

        with connection.cursor() as cursor:
            for topic, config in TABLES.items():
                cursor.execute(f"SELECT count(*) FROM {config['table']}")
                log.info("%s : %d lus, %d invalides, %d upserts -> %s contient %d lignes",
                         topic, stats[topic]["read"], stats[topic]["invalid"],
                         stats[topic]["upserted"], config["table"], cursor.fetchone()[0])
    finally:
        consumer.close()
        connection.close()


if __name__ == "__main__":
    try:
        main()
    except (KafkaException, psycopg2.Error) as exc:
        log.error("Échec du chargement : %s", exc)
        sys.exit(1)
