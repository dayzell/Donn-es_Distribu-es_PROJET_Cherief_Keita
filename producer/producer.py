"""Producer (API Collector du cahier des charges) : DummyJSON -> Kafka.

Collecte les produits et les clients DummyJSON page par page, les normalise
selon docs/etape-2-exploration-dummyjson.md, puis les publie dans
retail-products et retail-customers (clé = identifiant DummyJSON).
"""
import json
import logging
import os
import sys
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
API_BASE_URL = os.getenv("DUMMYJSON_BASE_URL", "https://dummyjson.com")
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "50"))
HTTP_TIMEOUT = (5, 15)  # (connexion, lecture) en secondes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s producer %(message)s",
)
log = logging.getLogger()


class IncompleteResponse(Exception):
    """La collecte n'a pas récupéré autant d'éléments que le total annoncé."""


def http_session():
    # Retries avec backoff sur les erreurs transitoires (429, 5xx).
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


def fetch_all(session, resource):
    """Parcourt la pagination (limit/skip) jusqu'à atteindre le total annoncé."""
    items, skip, total = [], 0, None
    while total is None or skip < total:
        response = session.get(
            f"{API_BASE_URL}/{resource}",
            params={"limit": PAGE_SIZE, "skip": skip},
            timeout=HTTP_TIMEOUT,
        )
        response.raise_for_status()
        page = response.json()
        batch = page.get(resource)
        if not isinstance(batch, list) or "total" not in page:
            raise IncompleteResponse(f"{resource} : réponse sans tableau ou sans total")
        total = page["total"]
        if not batch:
            break
        items.extend(batch)
        skip += len(batch)

    if len(items) != total:
        raise IncompleteResponse(f"{resource} : {len(items)} reçus pour un total de {total}")
    log.info("%s : %d éléments récupérés (total annoncé : %d)", resource, len(items), total)
    return items


def normalize_product(raw, collected_at):
    """Retourne (produit normalisé, None) ou (None, raison du rejet)."""
    product_id, title, category, price = (
        raw.get("id"), raw.get("title"), raw.get("category"), raw.get("price"),
    )
    if not isinstance(product_id, int):
        return None, "id manquant"
    if not title or not category:
        return None, "title ou category manquant"
    if not isinstance(price, (int, float)) or price < 0:
        return None, "price manquant ou négatif"
    stock = raw.get("stock")
    return {
        "product_id": product_id,
        "title": title.strip(),
        "category": category.strip(),
        "price": round(float(price), 2),
        "stock": stock if isinstance(stock, int) and stock >= 0 else None,
        "brand": (raw.get("brand") or "").strip() or None,  # absent pour ~la moitié des produits
        "source": "dummyjson",
        "collected_at": collected_at,
    }, None


def normalize_customer(raw, collected_at):
    """Retourne (client normalisé, None) ou (None, raison du rejet).

    Seuls les champs utiles sont conservés : password, ssn, bank, crypto, ip...
    ne quittent jamais le collecteur (minimisation des données).
    """
    customer_id, email = raw.get("id"), raw.get("email")
    first_name, last_name = raw.get("firstName"), raw.get("lastName")
    if not isinstance(customer_id, int):
        return None, "id manquant"
    if not email or "@" not in email:
        return None, "email manquant ou invalide"
    if not first_name or not last_name:
        return None, "nom ou prénom manquant"
    return {
        "customer_id": customer_id,
        "first_name": first_name.strip(),
        "last_name": last_name.strip(),
        "email": email.strip().lower(),
        "city": ((raw.get("address") or {}).get("city") or "").strip() or None,
        "source": "dummyjson",
        "collected_at": collected_at,
    }, None


def normalize_all(raw_items, normalize, collected_at, label):
    valid, rejected = [], 0
    for raw in raw_items:
        record, reason = normalize(raw, collected_at)
        if record is None:
            rejected += 1
            log.warning("%s rejeté (id=%s) : %s", label, raw.get("id"), reason)
        else:
            valid.append(record)
    log.info("%s : %d valides, %d rejetés", label, len(valid), rejected)
    return valid


def publish(producer, topic, records, key_field):
    errors = []

    def on_delivery(err, msg):
        if err is not None:
            errors.append(err)

    for record in records:
        producer.produce(
            topic,
            key=str(record[key_field]),
            value=json.dumps(record, ensure_ascii=False),
            on_delivery=on_delivery,
        )
        producer.poll(0)  # déclenche les callbacks de livraison au fil de l'eau

    remaining = producer.flush(30)
    if remaining or errors:
        raise RuntimeError(
            f"{topic} : {remaining} message(s) non livré(s), {len(errors)} erreur(s)"
        )
    log.info("%s : %d messages publiés", topic, len(records))


def main():
    collected_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    log.info("Collecte DummyJSON depuis %s (page de %d)", API_BASE_URL, PAGE_SIZE)

    # 1. Tout collecter et valider avant de publier : pas de publication partielle.
    with http_session() as session:
        products = normalize_all(
            fetch_all(session, "products"), normalize_product, collected_at, "produit"
        )
        customers = normalize_all(
            fetch_all(session, "users"), normalize_customer, collected_at, "client"
        )

    # 2. Publication : acks=all + idempotence (pas de doublon dû aux retries du producer).
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
    })
    publish(producer, "retail-products", products, "product_id")
    publish(producer, "retail-customers", customers, "customer_id")

    log.info(
        "Collecte terminée (collected_at=%s) : %d produits, %d clients publiés",
        collected_at, len(products), len(customers),
    )


if __name__ == "__main__":
    try:
        main()
    except (requests.RequestException, IncompleteResponse, RuntimeError) as exc:
        log.error("Échec de la collecte : %s", exc)
        sys.exit(1)
