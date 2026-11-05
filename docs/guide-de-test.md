# Guide de test — ce qui est réalisé (étapes 02 à 09)

Ce guide permet de rejouer tous les tests de la chaîne :
DummyJSON → producer → Kafka → consumer → PostgreSQL, puis simulator → Kafka → Spark → PostgreSQL.

Les commandes se lancent depuis la racine du projet, dans **Git Bash** (terminal de VS Code sous Windows). Sous WSL, Linux ou macOS, elles fonctionnent aussi : la ligne `export MSYS_NO_PATHCONV=1` y est simplement inutile.

## 0. Préparation (une fois par terminal)
```bash
export MSYS_NO_PATHCONV=1      # évite que Git Bash déforme les chemins (/opt/..., ./tests:/tests)

# Raccourcis utilisés dans les tests ci-dessous
sql()   { docker exec retailpulse-postgres sh -c "psql -U \$POSTGRES_USER -d \$POSTGRES_DB -c \"$1\""; }
check() { docker compose run --rm --no-deps -v ./tests:/tests --entrypoint python simulator /tests/check_pipeline.py; }
send()  { docker exec -i retailpulse-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic retail-order-events --property parse.key=true --property "key.separator=|"; }
```
- `sql "<requête>"` : exécute une requête dans PostgreSQL (sans guillemets doubles dans la requête).
- `check` : lance [tests/check_pipeline.py](../tests/check_pipeline.py), qui recalcule le résultat attendu depuis Kafka (sans passer par Spark) et le compare à PostgreSQL.
- `send` : publie dans `retail-order-events` des lignes au format `clé|json`.

## 1. Démarrer depuis zéro
```bash
docker compose down -v           # repart d'une base et d'un Kafka vides
docker compose up -d --build
```
Attendre environ deux minutes (plus au premier lancement : téléchargement de l'image Spark), puis :
```bash
docker compose ps -a
```
✅ Attendu :

| Conteneur | État |
|---|---|
| `retailpulse-kafka`, `retailpulse-postgres`, `retailpulse-spark` | `Up (healthy)` |
| `retailpulse-simulator` | `Up` |
| `retailpulse-kafka-init`, `retailpulse-producer`, `retailpulse-consumer` | `Exited (0)` (traitements ponctuels terminés) |

## 2. Infrastructure Kafka et PostgreSQL
```bash
docker exec retailpulse-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe | grep PartitionCount
sql "\dt"
```
✅ Attendu :
- 4 topics : `retail-products` et `retail-customers` (1 partition, `cleanup.policy=compact`), `retail-orders` et `retail-order-events` (3 partitions) ;
- 5 tables : `products`, `customers`, `orders`, `sales_aggregates`, `invalid_events`.

## 3. Référentiels (étape 03)
```bash
docker compose logs producer consumer
sql "SELECT (SELECT count(*) FROM products) produits, (SELECT count(*) FROM customers) clients, (SELECT count(DISTINCT category) FROM products) categories"
docker exec retailpulse-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic retail-customers --from-beginning --max-messages 1
```
✅ Attendu :
- logs du producer : « 194 éléments récupérés », « 208 éléments récupérés », 0 rejeté ;
- 194 produits, 208 clients, 24 catégories ;
- le message client ne contient **ni** `password`, **ni** `ssn`, **ni** `bank` (minimisation des données).

### Rejeu sans doublon
```bash
docker compose run --rm producer
docker compose run --rm --no-deps consumer
sql "SELECT (SELECT count(*) FROM products) produits, (SELECT count(*) FROM customers) clients"
```
✅ Attendu : toujours 194 et 208. Les logs du consumer montrent plus de messages lus (anciennes versions pas encore compactées), mais l'upsert ne crée aucune ligne en double.

## 4. Simulateur de commandes (étape 05)
```bash
docker compose logs --tail 5 simulator
docker exec retailpulse-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic retail-order-events --max-messages 3 --property print.key=true --property print.partition=true
```
✅ Attendu : statistiques toutes les 20 commandes ; événements contenant `event_id`, `event_type`, `event_timestamp`, `order_id`, `customer_id`, `items`, `amount`, `currency`. La clé affichée est l'`order_id`.

### Arrêt propre
```bash
docker compose stop simulator
docker compose logs --tail 2 simulator
```
✅ Attendu : `Arrêt propre : {...} ; 0 message(s) non livré(s)`.

## 5. Spark : résultats justes — flux nominal et paiements échoués (étape 06)
Simulateur arrêté, attendre 20 secondes (fin des micro-batches en cours), puis :
```bash
check
sql "SELECT status, count(*) FROM orders GROUP BY status"
sql "SELECT category, sum(total_revenue) ca FROM sales_aggregates GROUP BY 1 ORDER BY 2 DESC LIMIT 5"
```
✅ Attendu : `OK` sur toutes les lignes du contrôle. Le contrôle ne compte que `PAYMENT_COMPLETED` : l'égalité prouve que `PAYMENT_FAILED` et `ORDER_CANCELLED` n'augmentent pas le chiffre d'affaires.

Interface Spark : **http://localhost:4040**, onglet *Structured Streaming* (débit, durée des micro-batches).

## 6. Doublons
```bash
docker compose run --rm --no-deps -e MAX_ORDERS=10 -e DUPLICATE_RATE=1 simulator
sleep 25
check
```
✅ Attendu : la première ligne indique `10 doublon(s)` dans Kafka, et le contrôle reste `OK` partout : chaque paiement n'est compté qu'une fois.

## 7. Reprise de Spark après redémarrage (checkpoint)
```bash
sql "SELECT sum(total_revenue) FROM sales_aggregates"
docker compose restart spark
sleep 60
sql "SELECT sum(total_revenue) FROM sales_aggregates"
docker compose logs --since 70s spark | grep -E "orders|aggregates"
check
```
✅ Attendu : chiffre d'affaires identique avant et après, aucun micro-batch ne retraite les anciennes données, contrôle `OK`.

## 8. Messages invalides
```bash
printf '%s\n' \
 'k1|ceci nest pas du json' \
 'k2|{"event_id":"x1","event_type":"PAYMENT_COMPLETED"}' \
 'k3|{"event_id":"x2","event_type":"REFUND","event_timestamp":"2026-11-02T15:00:00Z","order_id":"o","customer_id":1,"items":[{"product_id":1,"quantity":1,"unit_price":1}],"amount":1}' | send
sleep 25
sql "SELECT reason, left(raw_value,50) FROM invalid_events"
```
✅ Attendu : 3 lignes, raisons `json_invalide_ou_champ_manquant` (deux fois) et `type_inattendu` ; chiffre d'affaires inchangé.

## 9. Événements tardifs (watermark de 10 minutes)
Le watermark suit l'événement le plus récent vu par Spark : relancer d'abord le simulateur pour l'amener à l'heure actuelle.
```bash
docker compose start simulator; sleep 30; docker compose stop simulator; sleep 20

ev() {  # $1 = minutes de retard, $2 = quantité du produit 1 (9,99 $)
  T=$(date -u -d "-$1 min" +%Y-%m-%dT%H:%M:%S.000Z)
  echo "ORD-LATE-$1|{\"event_id\":\"late-$1-$(date +%s)\",\"event_type\":\"PAYMENT_COMPLETED\",\"event_timestamp\":\"$T\",\"order_id\":\"ORD-LATE-$1\",\"customer_id\":1,\"items\":[{\"product_id\":1,\"quantity\":$2,\"unit_price\":9.99}],\"amount\":0,\"currency\":\"USD\"}"
}
sql "SELECT sum(total_revenue) avant FROM sales_aggregates"
{ ev 3 10; ev 30 7; } | send
sleep 25
sql "SELECT sum(total_revenue) apres FROM sales_aggregates"
```
✅ Attendu : le chiffre d'affaires augmente de **99,90** exactement. Le paiement en retard de 3 minutes (10 × 9,99) est compté ; celui en retard de 30 minutes (7 × 9,99 = 69,93) est ignoré car il dépasse le watermark.

## 10. Tableau de bord (étape 08)
Ouvrir **http://localhost:8501** dans un navigateur.
```bash
docker compose ps dashboard                      # Up (healthy)
sql "SELECT sum(total_revenue) ca, sum(total_quantity) articles FROM sales_aggregates"
sql "SELECT count(*) commandes, count(*) FILTER (WHERE status='PAID') payees FROM orders"
```
✅ Attendu :
- bandeau vert « Données à jour » ;
- les indicateurs de la page égaux aux résultats SQL (à quelques commandes près si le simulateur tourne) ;
- **critère d'acceptation** : simulateur actif (`docker compose start simulator`), les commandes, le CA et le tableau « Dernières commandes » changent **sans recharger la page** (la page est relue toutes les 60 s ; les chiffres ne bougent qu'à chaque micro-batch de Spark, toutes les 10 s avec `SPARK_TRIGGER_INTERVAL=10 seconds`, toutes les 5 min par défaut) ;
- simulateur arrêté depuis plus de 10 minutes (seuil `STALE_AFTER_SECONDS`, 600 s) : bandeau orange « Données figées ».

## 11. PostgreSQL indisponible
```bash
docker compose start simulator
docker compose stop postgres          # la page affiche « Connexion à PostgreSQL impossible »
sleep 60
docker compose ps -a                  # spark peut redémarrer : c'est attendu
docker compose start postgres
sleep 120                             # la page revient d'elle-même ; Spark rattrape son retard
docker compose stop simulator; sleep 40
check
```
✅ Attendu : aucun plantage du tableau de bord, Spark relancé automatiquement par Docker, puis `OK` partout au contrôle (aucune perte, aucun double comptage). Résultats détaillés : [tests-etape-08-dashboard.md](tests-etape-08-dashboard.md).

## 12. Kubernetes (étape 09, minikube)
Prérequis : `minikube start`, puis `docker compose down` (sans `-v`) pour libérer la mémoire.
```bash
bash k8s/deploy.sh                       # ~10 min la première fois
kubectl -n retailpulse get statefulsets,deployments,jobs,pvc
minikube service dashboard -n retailpulse
```
✅ Attendu : StatefulSets et Deployments `1/1`, Jobs `Complete`, PVC `Bound` ; tableau de bord à jour.

### Pod indisponible (section 13 du cahier)
```bash
kubectl -n retailpulse delete pod -l app.kubernetes.io/name=spark      # puis postgres, puis kafka
kubectl -n retailpulse get pods -w                                      # le pod est recréé (Ctrl+C pour quitter)
```
✅ Attendu : chaque pod recréé par son contrôleur en moins d'une minute, données conservées (PVC), flux repris.

### Contrôle d'intégrité dans le cluster
```bash
kubectl -n retailpulse scale deployment/simulator --replicas=0; sleep 40
kubectl -n retailpulse create configmap pipeline-check --from-file=tests/check_pipeline.py --dry-run=client -o yaml | kubectl apply -f -
kubectl -n retailpulse delete job check-pipeline --ignore-not-found; kubectl apply -f k8s/jobs/check-pipeline-job.yaml
kubectl -n retailpulse wait --for=condition=complete job/check-pipeline --timeout=120s; kubectl -n retailpulse logs job/check-pipeline
kubectl -n retailpulse scale deployment/simulator --replicas=1
```
✅ Attendu : `OK` sur toutes les lignes. Résultats obtenus : [deploiement-kubernetes.md](deploiement-kubernetes.md).

## 13. Remettre à zéro
Les tests 8 et 9 injectent volontairement des données que `check` signale ensuite comme `ÉCART` : c'est le comportement voulu. Pour repartir de données propres :
```bash
docker compose down -v
docker compose up -d --build
```

## En cas de problème
| Symptôme | Cause probable |
|---|---|
| Un dossier au nom bizarre (`tests;C`…) apparaît | `export MSYS_NO_PATHCONV=1` oublié dans ce terminal ; supprimer le dossier |
| `port is already allocated` | 9092, 5432 ou 4040 déjà utilisé : changer le port dans `.env` |
| `check` affiche `ÉCART` juste après un test | le simulateur tournait encore : l'arrêter, attendre 20 s, relancer `check` |
| Un conteneur est en `Exited (1)` | `docker compose logs <service>` affiche l'erreur |
| `sql: command not found` | l'étape 0 n'a pas été faite dans ce terminal |

Résultats obtenus lors de nos propres tests : [tests-etape-06-spark.md](tests-etape-06-spark.md).
