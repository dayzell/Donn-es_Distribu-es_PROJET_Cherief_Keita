"""Vérifie que PostgreSQL contient exactement ce que Kafka permet d'attendre.

Relit tout retail-order-events, applique les règles métier de façon indépendante
de Spark (PAYMENT_COMPLETED seulement, un event_id compté une seule fois), puis
compare avec sales_aggregates et orders. Lancement (réutilise l'image du simulator) :

    docker compose run --rm --no-deps -v ./tests:/tests --entrypoint python simulator /tests/check_pipeline.py
"""
import json
import os
import sys
from collections import Counter

import psycopg2
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

TOPIC = "retail-order-events"
STATUS = {"PAYMENT_COMPLETED": "PAID", "PAYMENT_FAILED": "FAILED", "ORDER_CANCELLED": "CANCELLED"}


def read_topic():
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        "group.id": "check-pipeline",
        "enable.auto.commit": False,
    })
    partitions = consumer.list_topics(TOPIC, timeout=15).topics[TOPIC].partitions
    remaining, assigned = {}, []
    for p in partitions:
        _, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=15)
        assigned.append(TopicPartition(TOPIC, p, OFFSET_BEGINNING))
        if high > 0:
            remaining[p] = high
    consumer.assign(assigned)
    messages = []
    while remaining:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        messages.append(msg.value())
        if msg.offset() + 1 >= remaining.get(msg.partition(), 0):
            remaining.pop(msg.partition(), None)
    consumer.close()
    return messages


def main():
    messages = read_topic()
    events, unreadable = [], 0
    for value in messages:
        try:
            events.append(json.loads(value))
        except (TypeError, ValueError):
            unreadable += 1

    unique = {}
    for e in events:
        if isinstance(e, dict) and e.get("event_id") and e.get("event_type") in STATUS:
            unique.setdefault(e["event_id"], e)
    completed = [e for e in unique.values() if e["event_type"] == "PAYMENT_COMPLETED"]
    expected_revenue = round(sum(i["quantity"] * i["unit_price"]
                                 for e in completed for i in e["items"]), 2)
    expected_quantity = sum(i["quantity"] for e in completed for i in e["items"])
    expected_status = Counter(STATUS[e["event_type"]] for e in unique.values())

    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"), user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"], dbname=os.environ["POSTGRES_DB"],
    )
    with connection, connection.cursor() as cursor:
        cursor.execute("SELECT coalesce(sum(total_revenue), 0), coalesce(sum(total_quantity), 0) "
                       "FROM sales_aggregates")
        revenue, quantity = cursor.fetchone()
        cursor.execute("SELECT status, count(*) FROM orders WHERE status <> 'CREATED' GROUP BY status")
        status = Counter(dict(cursor.fetchall()))
        cursor.execute("SELECT reason, count(*) FROM invalid_events GROUP BY reason ORDER BY reason")
        invalid = cursor.fetchall()
    connection.close()

    print(f"Kafka : {len(messages)} messages, {len(events) - len(unique)} doublon(s) ou hors contrat, "
          f"{unreadable} illisible(s), {len(completed)} paiements réussis uniques")
    rows = [
        ("Chiffre d'affaires", f"{expected_revenue:.2f}", f"{float(revenue):.2f}"),
        ("Quantités vendues", expected_quantity, quantity),
        ("Commandes PAID", expected_status["PAID"], status["PAID"]),
        ("Commandes FAILED", expected_status["FAILED"], status["FAILED"]),
        ("Commandes CANCELLED", expected_status["CANCELLED"], status["CANCELLED"]),
    ]
    ok = True
    print(f"{'Indicateur':<22}{'Attendu (Kafka)':>18}{'PostgreSQL':>16}")
    for label, expected, actual in rows:
        same = str(expected) == str(actual)
        ok &= same
        print(f"{label:<22}{expected!s:>18}{actual!s:>16}  {'OK' if same else 'ÉCART'}")
    print("Messages rejetés :", dict(invalid) or "aucun")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
