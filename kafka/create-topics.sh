#!/bin/bash
# Crée les topics RetailPulse (idempotent : --if-not-exists). Exécuté par le service kafka-init.
set -euo pipefail

BS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
T=/opt/kafka/bin/kafka-topics.sh

create() {
  "$T" --bootstrap-server "$BS" --create --if-not-exists --replication-factor 1 "$@"
}

# Référentiels : 1 partition, compactés (Kafka garde la dernière version de chaque clé).
create --topic retail-products  --partitions 1 --config cleanup.policy=compact
create --topic retail-customers --partitions 1 --config cleanup.policy=compact

# Événements : 3 partitions, clé = order_id (ordre garanti pour une même commande).
create --topic retail-orders       --partitions 3
create --topic retail-order-events --partitions 3

echo "Topics disponibles :"
"$T" --bootstrap-server "$BS" --list
