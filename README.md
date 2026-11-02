# RetailPulse — plateforme distribuée e-commerce en quasi-temps réel

Projet Master 2 Data — module Données distribuées. Chaîne cible (cahier des charges) :

```
API DummyJSON → producer → Kafka → consumer → PostgreSQL     (référentiels : fait)
simulator → Kafka                                             (commandes et paiements : fait)
Kafka → Spark → PostgreSQL → dashboard                        (à venir)
```

## État d'avancement (démarche du cahier des charges, section 16)
| Étape | Contenu | État |
|---|---|---|
| 02 | Exploration de DummyJSON | fait — [docs/etape-2-exploration-dummyjson.md](docs/etape-2-exploration-dummyjson.md) |
| 03 | Référentiels : collecte DummyJSON, chargement PostgreSQL, rejeu | **fait** |
| 04 | Kafka KRaft : broker, topics, partitions, clés | fait (topics de commandes prêts) |
| 05 | Simulation commerciale (commandes, paiements) | **fait** |
| 06 | Spark Structured Streaming | à faire |
| 07 | PostgreSQL : schéma, contraintes, index, upsert | schéma fait, upsert des agrégats à faire |
| 08 | Tableau de bord | à faire |
| 09 | Docker et Kubernetes | Docker fait pour les services existants, Kubernetes à faire |
| 10 | Tests et soutenance | à faire |

## Structure du dépôt
Un dossier par service, nommé d'après ce qu'il contient.
```
.
├── docker-compose.yml          # lance tous les conteneurs
├── .env.example                # modèle de configuration (le vrai .env n'est jamais versionné)
├── kafka/
│   └── create-topics.sh        # création des topics (service kafka-init)
├── postgres/
│   └── init/01_schema.sql      # schéma PostgreSQL
├── producer/                   # Python : DummyJSON -> Kafka (« API Collector » du cahier)
│   ├── producer.py  Dockerfile  requirements.txt
├── consumer/                   # Python : Kafka -> PostgreSQL (« Reference Loader » du cahier)
│   ├── consumer.py  Dockerfile  requirements.txt
├── simulator/                  # Python : commandes et paiements simulés -> Kafka (« Producteur » du cahier)
│   ├── simulator.py  Dockerfile  requirements.txt
└── docs/                       # rapports et dossier d'architecture
```

## Démarrage
Prérequis : Docker et Docker Compose.
```bash
cp .env.example .env          # puis choisir un mot de passe PostgreSQL dans .env
docker compose up -d --build
docker compose ps -a
```
Résultat attendu (environ une minute) :

| Conteneur | État attendu | Rôle |
|---|---|---|
| `retailpulse-kafka` | `Up (healthy)` | broker Kafka 4.0 en mode KRaft |
| `retailpulse-postgres` | `Up (healthy)` | base PostgreSQL 16 |
| `retailpulse-kafka-init` | `Exited (0)` | a créé les topics, puis s'est arrêté |
| `retailpulse-producer` | `Exited (0)` | a collecté DummyJSON et publié dans Kafka |
| `retailpulse-consumer` | `Exited (0)` | a chargé Kafka dans PostgreSQL |
| `retailpulse-simulator` | `Up` | génère des commandes et des paiements en continu |

`Exited (0)` est normal pour kafka-init, producer et consumer : ce sont des traitements ponctuels (des « Jobs »), pas des services permanents.

## Ce que fait chaque service

### kafka + kafka-init
- Kafka 4.0, mode KRaft (sans ZooKeeper), un seul broker.
- Deux adresses : `kafka:29092` entre conteneurs, `localhost:9092` depuis la machine hôte.
- Création automatique des topics désactivée : seuls les topics de `kafka/create-topics.sh` existent.

| Topic | Partitions | Clé | Rôle |
|---|---|---|---|
| `retail-products` | 1, compacté | `product_id` | catalogue DummyJSON |
| `retail-customers` | 1, compacté | `customer_id` | clients DummyJSON |
| `retail-orders` | 3 | `order_id` | `ORDER_CREATED` (étape 05) |
| `retail-order-events` | 3 | `order_id` | paiements et annulations (étape 05) |

Replication factor 1 : un seul broker, aucune copie possible. Les topics de référence sont compactés : Kafka finit par ne garder que la dernière version de chaque produit ou client.

### producer (`producer/producer.py`)
1. Parcourt `https://dummyjson.com/products` et `/users` page par page (`limit` / `skip`) jusqu'au `total` annoncé.
2. Gère les erreurs : timeout (5 s connexion, 15 s lecture), 5 nouvelles tentatives avec attente croissante sur les erreurs 429 et 5xx, réponse incomplète (nombre reçu ≠ `total`).
3. Valide et normalise (règles de [l'étape 2](docs/etape-2-exploration-dummyjson.md)) : champs utiles seulement, noms en `snake_case`, `brand` à `null` si absent, email en minuscules. Les champs sensibles (`password`, `ssn`, `bank`…) ne sont jamais publiés.
4. Publie seulement si toute la collecte a réussi : pas de publication partielle.
5. Producer Kafka avec `acks=all` et `enable.idempotence=True`, `flush()` final. Code de sortie 1 en cas d'échec.

Paramètres (variables d'environnement) : `KAFKA_BOOTSTRAP_SERVERS`, `DUMMYJSON_BASE_URL`, `PAGE_SIZE` (50 par défaut).

### consumer (`consumer/consumer.py`)
1. Relit `retail-products` et `retail-customers` depuis le début, jusqu'à la fin mesurée au démarrage, puis s'arrête.
2. Écrit par lots de 100 dans PostgreSQL avec un upsert (`INSERT … ON CONFLICT (id) DO UPDATE`).
3. Rejouable sans doublon : la clé primaire est l'identifiant DummyJSON. Relancer ne fait que mettre à jour les lignes.
4. Messages invalides (JSON illisible, identifiant manquant) : ignorés et comptés dans les logs.

Paramètres : `KAFKA_BOOTSTRAP_SERVERS`, `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `BATCH_SIZE` (100).

### simulator (`simulator/simulator.py`)
Les commandes et les paiements sont **simulés** : ils ne viennent pas de DummyJSON (principe fondamental du cahier).
1. Charge les produits et les clients depuis PostgreSQL (donc après le consumer).
2. Pour chaque commande : un client au hasard, 1 à 4 produits distincts, 1 à 3 unités chacun, prix catalogue.
3. Publie `ORDER_CREATED` dans `retail-orders`, puis le résultat dans `retail-order-events` quelques centaines de millisecondes plus tard :
   `PAYMENT_COMPLETED` (85 %), `PAYMENT_FAILED` (10 %) ou `ORDER_CANCELLED` (5 %).
4. Attend 0,5 à 2 secondes, puis recommence jusqu'au signal d'arrêt.
5. Arrêt propre sur `docker compose stop simulator` (SIGTERM) : termine la commande en cours, vide le tampon Kafka (`flush`), affiche ses statistiques et sort avec le code 0.

Règle de chiffre d'affaires : seul `PAYMENT_COMPLETED` compte. Une commande `ORDER_CANCELLED` est annulée **avant** paiement : un paiement réussi n'est jamais annulé ensuite, donc aucune règle de compensation n'est nécessaire.

| Paramètre | Défaut | Rôle |
|---|---|---|
| `INTERVAL_MIN` / `INTERVAL_MAX` | 0.5 / 2.0 | secondes entre deux commandes |
| `MAX_ITEMS` / `MAX_QUANTITY` | 4 / 3 | lignes par commande / unités par ligne |
| `FAILURE_RATE` / `CANCEL_RATE` | 0.10 / 0.05 | part de paiements échoués / de commandes annulées |
| `MAX_ORDERS` | 0 | nombre de commandes avant arrêt (0 = sans limite) |
| `SEED` | — | graine aléatoire, pour une simulation reproductible |
| `DUPLICATE_RATE` | 0 | test : republie un paiement avec le même `event_id` |
| `LATE_RATE` / `LATE_MINUTES` | 0 / 30 | test : date un événement dans le passé |

Dans `docker-compose.yml`, les cinq premiers se règlent depuis `.env` (`SIM_INTERVAL_MIN`, `SIM_FAILURE_RATE`…). Exemple de test des doublons :
```bash
docker compose run --rm --no-deps -e MAX_ORDERS=10 -e DUPLICATE_RATE=1 simulator
```

#### Contrat des événements (section 7 du cahier)
Même structure dans les deux topics ; clé Kafka = `order_id`.
```json
{
  "event_id": "7209acd3-bb63-4fd4-9c78-83fdef3cb93d",
  "event_type": "PAYMENT_COMPLETED",
  "event_timestamp": "2026-11-02T14:03:57.055Z",
  "order_id": "ORD-037ed2af-baec-4c6a-9cd0-0dfd50d46483",
  "customer_id": 92,
  "items": [{"product_id": 115, "quantity": 1, "unit_price": 14999.99}],
  "amount": 14999.99,
  "currency": "USD"
}
```
| Champ | Règle |
|---|---|
| `event_id` | UUID unique par événement ; clé de déduplication |
| `event_type` | `ORDER_CREATED`, `PAYMENT_COMPLETED`, `PAYMENT_FAILED`, `ORDER_CANCELLED` |
| `event_timestamp` | heure de production, ISO 8601 UTC, millisecondes, suffixe `Z` |
| `order_id` | `ORD-<uuid>`, relie la commande et son paiement |
| `customer_id` | identifiant DummyJSON du client |
| `items` | `product_id` DummyJSON, quantité, prix unitaire catalogue |
| `amount` / `currency` | montant **brut** (quantité × prix catalogue, sans remise), en USD |

La catégorie n'est volontairement pas dans l'événement : Spark l'ajoutera depuis la table `products` (enrichissement, étape 06). Les lignes sont aussi copiées dans les événements de paiement, pour que Spark calcule le CA par catégorie sans jointure entre deux flux.

### postgres (`postgres/init/01_schema.sql`)
Tables `products`, `customers`, `orders`, `sales_aggregates` (section 9 du cahier) et `invalid_events` (messages rejetés par Spark, étape 06). Le script ne s'exécute qu'à la première création du volume.

## Vérifier
Sous Git Bash (Windows), préfixer les commandes `docker exec` par `MSYS_NO_PATHCONV=1`.
```bash
# Logs du producer et du consumer
docker compose logs producer consumer

# Nombre de lignes chargées (attendu : 194 produits, 208 clients, 24 catégories)
docker exec retailpulse-postgres sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT (SELECT count(*) FROM products) produits, (SELECT count(*) FROM customers) clients, (SELECT count(DISTINCT category) FROM products) categories"'

# Un message dans Kafka
docker exec retailpulse-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic retail-products --from-beginning --max-messages 1 --property print.key=true
```

### Suivre le simulateur
```bash
docker compose logs -f simulator          # statistiques toutes les 20 commandes
docker exec retailpulse-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic retail-order-events --property print.key=true --property print.partition=true
docker compose stop simulator             # arrêt propre
docker compose start simulator            # reprise
```

### Tester le rejeu
```bash
docker compose run --rm producer              # nouvelle collecte
docker compose run --rm --no-deps consumer    # nouveau chargement (--no-deps : ne relance pas le producer)
```
Les compteurs restent à 194 et 208. Les logs du consumer montrent plus de messages lus (les anciennes versions ne sont pas encore effacées par la compaction), mais le même nombre de lignes en base.

## Arrêter
```bash
docker compose down        # conserve les données
docker compose down -v     # supprime aussi les volumes (le schéma SQL sera rejoué au prochain démarrage)
```

## Limites connues
- Un seul broker Kafka (RF=1) : pas de tolérance à la perte du broker.
- Les données DummyJSON sont fictives ; leurs volumes peuvent changer (ils ne sont pas codés en dur).
- Le consumer recharge tout le référentiel à chaque exécution : simple et rejouable, acceptable pour quelques centaines de lignes.
