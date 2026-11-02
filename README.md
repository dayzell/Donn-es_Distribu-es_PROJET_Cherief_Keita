# RetailPulse — plateforme distribuée e-commerce en quasi-temps réel

Projet Master 2 Data — module Données distribuées. Chaîne cible (cahier des charges) :

```
API DummyJSON → producer → Kafka → consumer → PostgreSQL          (référentiels : fait)
simulateur de commandes → Kafka → Spark → PostgreSQL → dashboard   (à venir)
```

## État d'avancement (démarche du cahier des charges, section 16)
| Étape | Contenu | État |
|---|---|---|
| 02 | Exploration de DummyJSON | fait — [docs/etape-2-exploration-dummyjson.md](docs/etape-2-exploration-dummyjson.md) |
| 03 | Référentiels : collecte DummyJSON, chargement PostgreSQL, rejeu | **fait** |
| 04 | Kafka KRaft : broker, topics, partitions, clés | fait (topics de commandes prêts) |
| 05 | Simulation commerciale (commandes, paiements) | à faire |
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

`Exited (0)` est normal pour les trois derniers : ce sont des traitements ponctuels (des « Jobs »), pas des services permanents.

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
