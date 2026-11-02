"""Simulateur commercial (Producteur Python, section 6.3 du cahier des charges).

Charge les produits et les clients depuis PostgreSQL, puis génère en continu :
- ORDER_CREATED dans retail-orders ;
- PAYMENT_COMPLETED, PAYMENT_FAILED ou ORDER_CANCELLED dans retail-order-events.
La clé Kafka est order_id : tous les événements d'une commande vont dans la
même partition, donc dans l'ordre. S'arrête proprement sur SIGTERM / Ctrl+C.
"""
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import psycopg2
from confluent_kafka import KafkaException, Producer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
ORDERS_TOPIC = "retail-orders"
EVENTS_TOPIC = "retail-order-events"
CURRENCY = "USD"  # devise des prix DummyJSON

# Paramètres de simulation (documentés dans le README)
INTERVAL_MIN = float(os.getenv("INTERVAL_MIN", "0.5"))    # secondes entre deux commandes
INTERVAL_MAX = float(os.getenv("INTERVAL_MAX", "2.0"))
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "4"))              # lignes par commande
MAX_QUANTITY = int(os.getenv("MAX_QUANTITY", "3"))        # quantité par ligne
FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0.10"))   # part de PAYMENT_FAILED
CANCEL_RATE = float(os.getenv("CANCEL_RATE", "0.05"))     # part de ORDER_CANCELLED
MAX_ORDERS = int(os.getenv("MAX_ORDERS", "0"))            # 0 = sans limite
SEED = os.getenv("SEED")                                  # rend la simulation reproductible
# Paramètres de test (étape 10) : 0 en fonctionnement normal.
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0"))  # republie un événement (même event_id)
LATE_RATE = float(os.getenv("LATE_RATE", "0"))            # événement daté dans le passé
LATE_MINUTES = int(os.getenv("LATE_MINUTES", "30"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s simulator %(message)s",
)
log = logging.getLogger()

running = True


def stop(signum, _frame):
    global running
    log.info("Signal %s reçu : arrêt après la commande en cours", signal.Signals(signum).name)
    running = False


def load_references():
    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT product_id, price FROM products WHERE price > 0")
            products = cursor.fetchall()
            cursor.execute("SELECT customer_id FROM customers")
            customers = [row[0] for row in cursor.fetchall()]
    finally:
        connection.close()
    if not products or not customers:
        raise RuntimeError("référentiels vides : lancer d'abord le producer puis le consumer")
    return [(pid, float(price)) for pid, price in products], customers


def timestamp(moment):
    """Format documenté : ISO 8601 UTC, millisecondes, suffixe Z."""
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_order(products, customers):
    lines = random.sample(products, k=random.randint(1, min(MAX_ITEMS, len(products))))
    items = [
        {"product_id": pid, "quantity": random.randint(1, MAX_QUANTITY), "unit_price": price}
        for pid, price in lines
    ]
    return {
        "order_id": f"ORD-{uuid.uuid4()}",
        "customer_id": random.choice(customers),
        "items": items,
        # Montant brut : somme quantité x prix catalogue, sans remise.
        "amount": round(sum(i["quantity"] * i["unit_price"] for i in items), 2),
        "currency": CURRENCY,
    }


def build_event(event_type, order, moment):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": timestamp(moment),
        **order,
    }


def payment_outcome():
    draw = random.random()
    if draw < CANCEL_RATE:
        return "ORDER_CANCELLED"       # annulée avant paiement : jamais de CA à compenser
    if draw < CANCEL_RATE + FAILURE_RATE:
        return "PAYMENT_FAILED"
    return "PAYMENT_COMPLETED"


def main():
    if SEED is not None:
        random.seed(int(SEED))
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    products, customers = load_references()
    log.info("Référentiels chargés : %d produits, %d clients", len(products), len(customers))
    log.info("Paramètres : intervalle %.1f-%.1f s, échec %.0f %%, annulation %.0f %%, max %s commandes",
             INTERVAL_MIN, INTERVAL_MAX, FAILURE_RATE * 100, CANCEL_RATE * 100, MAX_ORDERS or "∞")

    errors = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(err)
            log.error("Échec de livraison : %s", err)

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
    })

    def publish(topic, event):
        producer.produce(topic, key=event["order_id"],
                         value=json.dumps(event), on_delivery=on_delivery)

    stats = {"orders": 0, "PAYMENT_COMPLETED": 0, "PAYMENT_FAILED": 0,
             "ORDER_CANCELLED": 0, "duplicates": 0, "late": 0}

    while running and (MAX_ORDERS == 0 or stats["orders"] < MAX_ORDERS):
        now = datetime.now(timezone.utc)
        if random.random() < LATE_RATE:
            now -= timedelta(minutes=LATE_MINUTES)
            stats["late"] += 1

        order = build_order(products, customers)
        publish(ORDERS_TOPIC, build_event("ORDER_CREATED", order, now))

        outcome = payment_outcome()
        # Le résultat du paiement arrive quelques centaines de millisecondes après la commande.
        payment = build_event(outcome, order, now + timedelta(milliseconds=random.randint(100, 900)))
        publish(EVENTS_TOPIC, payment)
        if random.random() < DUPLICATE_RATE:
            publish(EVENTS_TOPIC, payment)  # même event_id : Spark doit le dédupliquer
            stats["duplicates"] += 1

        producer.poll(0)
        stats["orders"] += 1
        stats[outcome] += 1
        if stats["orders"] % 20 == 0:
            log.info("Statistiques : %s", stats)

        time.sleep(random.uniform(INTERVAL_MIN, INTERVAL_MAX))

    remaining = producer.flush(15)
    log.info("Arrêt propre : %s ; %d message(s) non livré(s)", stats, remaining)
    if remaining or errors:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except (KafkaException, psycopg2.Error, RuntimeError) as exc:
        log.error("Échec du simulateur : %s", exc)
        sys.exit(1)
