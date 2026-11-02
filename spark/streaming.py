"""Traitement Spark Structured Streaming (section 8 du cahier des charges).

Lit retail-orders et retail-order-events, puis lance trois requêtes streaming,
chacune avec son propre checkpoint :
- invalid    : messages rejetés (JSON, champs, type, produit inconnu) -> invalid_events
- orders     : détail des commandes et de leur statut -> orders (upsert)
- aggregates : CA par catégorie et fenêtre de 5 minutes, PAYMENT_COMPLETED
               uniquement, dédupliqué par event_id -> sales_aggregates (upsert)
"""
import logging
import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, StringType, StructField, StructType,
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
ORDERS_TOPIC = "retail-orders"
EVENTS_TOPIC = "retail-order-events"
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/checkpoints")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 minutes")
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "5 minutes")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")
MAX_OFFSETS_PER_TRIGGER = os.getenv("MAX_OFFSETS_PER_TRIGGER", "5000")

PG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "dbname": os.environ["POSTGRES_DB"],
}
JDBC_URL = f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}"

PAYMENT_TYPES = ["PAYMENT_COMPLETED", "PAYMENT_FAILED", "ORDER_CANCELLED"]
ORDER_STATUS = {
    "ORDER_CREATED": "CREATED",
    "PAYMENT_COMPLETED": "PAID",
    "PAYMENT_FAILED": "FAILED",
    "ORDER_CANCELLED": "CANCELLED",
}

# Contrat des événements (README, section simulator)
ITEM_SCHEMA = StructType([
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
])
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", IntegerType()),
    StructField("items", ArrayType(ITEM_SCHEMA)),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
])

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s spark %(message)s")
log = logging.getLogger()
logging.getLogger("py4j").setLevel(logging.WARNING)  # pont Python/JVM très bavard en INFO

INSERT_INVALID = """
    INSERT INTO invalid_events (topic, kafka_partition, kafka_offset, reason, event_id, raw_value)
    VALUES %s
    ON CONFLICT (topic, kafka_partition, kafka_offset, reason) DO NOTHING
"""
# ORDER_CREATED fixe les données de la commande sans toucher au statut ;
# un événement de paiement ne change que le statut. L'ordre d'arrivée importe donc peu.
UPSERT_ORDER_CREATED = """
    INSERT INTO orders (order_id, customer_id, created_at, status, total_amount)
    VALUES %s
    ON CONFLICT (order_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id, created_at = EXCLUDED.created_at,
        total_amount = EXCLUDED.total_amount, updated_at = now()
"""
UPSERT_ORDER_STATUS = """
    INSERT INTO orders (order_id, customer_id, created_at, status, total_amount)
    VALUES %s
    ON CONFLICT (order_id) DO UPDATE SET status = EXCLUDED.status, updated_at = now()
"""
# Spark (mode update) renvoie la valeur complète de l'agrégat : on remplace, on n'additionne
# pas. Rejouer un micro-batch réécrit donc les mêmes valeurs (écriture idempotente).
UPSERT_AGGREGATES = """
    INSERT INTO sales_aggregates
        (category, window_start, window_end, order_count, total_quantity, total_revenue)
    VALUES %s
    ON CONFLICT (category, window_start, window_end) DO UPDATE SET
        order_count = EXCLUDED.order_count, total_quantity = EXCLUDED.total_quantity,
        total_revenue = EXCLUDED.total_revenue, updated_at = now()
"""


def read_events(spark):
    """Lit les deux topics, parse le JSON et ajoute une colonne reason (null = valide)."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", f"{ORDERS_TOPIC},{EVENTS_TOPIC}")
        .option("startingOffsets", "earliest")  # premier démarrage ; ensuite le checkpoint décide
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )
    events = (
        raw.select("topic", "partition", "offset", F.col("value").cast("string").alias("raw_value"))
        .withColumn("event", F.from_json("raw_value", EVENT_SCHEMA))
        .select("topic", "partition", "offset", "raw_value", "event.*")
        .withColumn("event_time", F.to_timestamp("event_timestamp"))
    )

    missing = F.lit(False)
    for field in ("event_id", "event_type", "event_timestamp", "order_id",
                  "customer_id", "items", "amount"):
        missing = missing | F.col(field).isNull()
    expected_type = (
        ((F.col("topic") == ORDERS_TOPIC) & (F.col("event_type") == "ORDER_CREATED"))
        | ((F.col("topic") == EVENTS_TOPIC) & F.col("event_type").isin(PAYMENT_TYPES))
    )
    bad_line = F.exists(
        "items",
        lambda i: i.product_id.isNull() | i.quantity.isNull() | (i.quantity <= 0)
        | i.unit_price.isNull() | (i.unit_price < 0),
    )
    reason = (
        F.when(missing, "json_invalide_ou_champ_manquant")
        .when(F.col("event_time").isNull(), "horodatage_invalide")
        .when(~expected_type, "type_inattendu")
        .when(F.col("amount") < 0, "montant_negatif")
        .when((F.size("items") == 0) | bad_line, "ligne_invalide")
    )
    return events.withColumn("reason", reason)


def read_products(spark):
    """Référentiel produits (table statique relue à chaque micro-batch : reste à jour)."""
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "products")
        .option("user", PG["user"])
        .option("password", PG["password"])
        .option("driver", "org.postgresql.Driver")
        .load()
        .select("product_id", "category")
    )


def run_sql(statements):
    """Exécute des (requête, lignes) dans une seule transaction PostgreSQL."""
    connection = psycopg2.connect(**PG)
    try:
        with connection, connection.cursor() as cursor:
            for sql, rows in statements:
                if rows:
                    execute_values(cursor, sql, rows)
    finally:
        connection.close()


def write_invalid(batch_df, batch_id):
    rows = [
        (r.topic, r.partition, r.offset, r.reason, r.event_id, r.raw_value)
        for r in batch_df.collect()
    ]
    if rows:
        run_sql([(INSERT_INVALID, rows)])
        log.info("invalid    batch %d : %d message(s) rejeté(s)", batch_id, len(rows))


def write_orders(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    connection = psycopg2.connect(**PG)
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT customer_id FROM customers WHERE customer_id = ANY(%s)",
                (list({r.customer_id for r in rows}),),
            )
            known = {row[0] for row in cursor.fetchall()}

            # Une commande peut apparaître deux fois dans un batch (doublon, rejeu) :
            # on garde un seul événement par commande, sinon l'upsert échoue.
            created, payments, unknown = {}, {}, []
            for r in rows:
                if r.customer_id not in known:
                    unknown.append((r.topic, r.partition, r.offset, "client_inconnu",
                                    r.event_id, r.raw_value))
                    continue
                target = created if r.event_type == "ORDER_CREATED" else payments
                previous = target.get(r.order_id)
                if previous is None or r.event_time >= previous.event_time:
                    target[r.order_id] = r

            def values(events):
                return [(r.order_id, r.customer_id, r.event_time,
                         ORDER_STATUS[r.event_type], round(r.amount, 2)) for r in events]

            for sql, data in ((INSERT_INVALID, unknown),
                              (UPSERT_ORDER_CREATED, values(created.values())),
                              (UPSERT_ORDER_STATUS, values(payments.values()))):
                if data:
                    execute_values(cursor, sql, data)
    finally:
        connection.close()
    log.info("orders     batch %d : %d créée(s), %d statut(s), %d client(s) inconnu(s)",
             batch_id, len(created), len(payments), len(unknown))


def write_aggregates(batch_df, batch_id):
    rows = [
        (r.category, r.window_start, r.window_end, r.order_count,
         r.total_quantity, round(r.total_revenue, 2))
        for r in batch_df.collect()
    ]
    if rows:
        run_sql([(UPSERT_AGGREGATES, rows)])
        log.info("aggregates batch %d : %d agrégat(s) mis à jour", batch_id, len(rows))


def start(query_df, name, writer, output_mode="append"):
    return (
        query_df.writeStream.queryName(name)
        .outputMode(output_mode)
        .foreachBatch(writer)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{name}")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


def main():
    spark = SparkSession.builder.appName("retailpulse-streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    events = read_events(spark)
    products = read_products(spark)
    valid = events.filter(F.col("reason").isNull())

    # Lignes de paiements réussis, une ligne par produit commandé.
    completed_lines = (
        valid.filter(F.col("event_type") == "PAYMENT_COMPLETED")
        .select("topic", "partition", "offset", "raw_value", "event_id", "order_id",
                "event_time", F.explode("items").alias("item"))
        .select("*", F.col("item.product_id").alias("product_id"))
    )

    # 1. Messages rejetés + lignes impossibles à enrichir (produit absent du référentiel).
    unknown_products = (
        completed_lines.join(products, "product_id", "left")
        .filter(F.col("category").isNull())
        .select("topic", "partition", "offset", F.lit("produit_inconnu").alias("reason"),
                "event_id", "raw_value")
    )
    invalid = (
        events.filter(F.col("reason").isNotNull())
        .select("topic", "partition", "offset", "reason", "event_id", "raw_value")
        .unionByName(unknown_products)
    )

    # 2. Détail des commandes.
    orders = valid.select("topic", "partition", "offset", "raw_value", "event_id",
                          "event_type", "order_id", "customer_id", "event_time", "amount")

    # 3. Chiffre d'affaires : PAYMENT_COMPLETED seulement, dédupliqué par event_id
    #    (état conservé le temps du watermark), enrichi par la catégorie, fenêtres de 5 min.
    aggregates = (
        valid.filter(F.col("event_type") == "PAYMENT_COMPLETED")
        .withWatermark("event_time", WATERMARK_DELAY)
        .dropDuplicatesWithinWatermark(["event_id"])
        .select("order_id", "event_time", F.explode("items").alias("item"))
        .select(
            "order_id", "event_time",
            F.col("item.product_id").alias("product_id"),
            F.col("item.quantity").alias("quantity"),
            (F.col("item.quantity") * F.col("item.unit_price")).alias("line_amount"),
        )
        .join(products, "product_id")  # inner : les lignes sans produit connu sont tracées en 1.
        .groupBy(F.window("event_time", WINDOW_DURATION), "category")
        .agg(
            F.size(F.collect_set("order_id")).alias("order_count"),
            F.sum("quantity").alias("total_quantity"),
            F.sum("line_amount").alias("total_revenue"),
        )
        .select(
            "category",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "order_count", "total_quantity", "total_revenue",
        )
    )

    start(invalid, "invalid", write_invalid)
    start(orders, "orders", write_orders)
    start(aggregates, "aggregates", write_aggregates, output_mode="update")
    log.info("3 requêtes démarrées (fenêtre %s, watermark %s, trigger %s, checkpoint %s)",
             WINDOW_DURATION, WATERMARK_DELAY, TRIGGER_INTERVAL, CHECKPOINT_DIR)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
