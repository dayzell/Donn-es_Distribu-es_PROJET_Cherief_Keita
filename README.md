# TP Kafka — Producer et Consumer Python avec un événement JSON

## Objectif
Démarrer un broker Kafka 4.0 en mode KRaft avec Docker Compose, publier un événement JSON dans `sales-events`, puis le consommer avec un groupe `sales-app`.

## Architecture du TP
- 1 broker Kafka (configuration pédagogique locale)
- KRaft, sans ZooKeeper
- Topic `sales-events`, 3 partitions, facteur de réplication RF=1
- Producer Python avec clé `store_id`
- Consumer Python avec commit manuel après traitement

## Prérequis
Docker Engine et le plugin Docker Compose.

## 1. Démarrer Kafka
```bash
docker compose up -d kafka
docker compose ps
docker compose logs -f kafka
```
Attendre que Kafka ait terminé son démarrage. Dans un autre terminal, construire l'image Python :
```bash
docker compose build producer consumer
```

## 2. Envoyer l'événement JSON
Lancer le producer en premier : il crée le topic `sales-events` s'il n'existe pas, lit `event.json`, puis publie le JSON.
```bash
docker compose run --rm producer
```
## 3. Consommer l'événement
Dans un deuxième terminal (ou après la fin du producer) :
```bash
docker compose run --rm consumer
```
Le consumer lit depuis le début du topic (`auto.offset.reset=earliest`), reçoit l'événement, puis s'arrête après l'avoir traité.

## 4. Résultat attendu
Le consumer affiche l'événement JSON, puis la partition et l'offset validé. Les numéros exacts de partition et d'offset dépendent de l'exécution.

## 5. Arrêter et nettoyer
```bash
docker compose down
```
Pour supprimer aussi les données Kafka persistées :
```bash
docker compose down -v
```

## Points pédagogiques
- La clé `store_id` permet à Kafka de répartir les événements d'un même magasin sur la même partition (avec le partitionneur par défaut).
- `acks=all` demande l'acquittement des réplicas synchronisés disponibles (ISR).
- `enable.idempotence=True` protège contre les doublons causés par les retries du producer dans le périmètre Kafka.
- `enable.auto.commit=False` et le commit après traitement illustrent une approche at-least-once : un message peut être retraité si le traitement réussit mais que le commit n'est pas enregistré.
- RF=1 est choisi pour la simplicité du TP. Il ne permet pas de démontrer la tolérance à la perte d'un broker.