# Étape 09 — Déploiement Kubernetes (minikube)

Déploiement réalisé le 3 novembre 2026 sur **minikube v1.39.0** (Kubernetes v1.37.0, driver docker, 4 CPU, 6 Go), depuis Git Bash sous Windows. Toute la plateforme tourne dans le cluster, dans le namespace `retailpulse`.

## Architecture déployée
| Composant | Ressource Kubernetes | Stockage / exposition | Ressources (requests → limits) |
|---|---|---|---|
| Kafka 4.0 KRaft | StatefulSet `kafka` (1 réplica) | PVC `data-kafka-0` 2 Gi ; Services `kafka` (ClusterIP 29092) et `kafka-headless` | 0,5 CPU / 1 Gi → 1 CPU / 1,5 Gi |
| PostgreSQL 16 | StatefulSet `postgres` | PVC `data-postgres-0` 1 Gi ; Service `postgres` (5432) | 0,25 CPU / 256 Mi → 0,5 CPU / 512 Mi |
| Création des topics | Job `kafka-init` | script `kafka/create-topics.sh` via ConfigMap `kafka-scripts` | |
| Collecte DummyJSON | Job `producer` | | 0,1 CPU / 128 Mi → 0,5 CPU / 256 Mi |
| Chargement des référentiels | Job `consumer` | | idem |
| Simulateur | Deployment `simulator` | | 0,1 CPU / 128 Mi → 0,25 CPU / 256 Mi |
| Spark Structured Streaming | Deployment `spark` (stratégie `Recreate`) | PVC `spark-checkpoints` 1 Gi ; Service `spark-ui` (4040) | 1 CPU / 1,5 Gi → 2 CPU / 2,5 Gi |
| Tableau de bord | Deployment `dashboard` | Service **NodePort** `dashboard` (30851) | 0,1 CPU / 256 Mi → 0,5 CPU / 512 Mi |
| Contrôle d'intégrité | Job `check-pipeline` (à la demande) | script `tests/check_pipeline.py` via ConfigMap | |

Configuration : ConfigMap `retailpulse-config` (paramètres) et Secret `retailpulse-db` (identifiants PostgreSQL), **créé par `k8s/deploy.sh` à partir de `.env`** : aucun secret dans les fichiers versionnés. Les connexions internes utilisent les noms DNS des Services (`kafka:29092`, `postgres:5432`).

Sondes : `tcpSocket` 29092 pour Kafka, `pg_isready` pour PostgreSQL, HTTP 4040 pour Spark, HTTP `/_stcore/health` pour le tableau de bord.

## Preuves de déploiement
Sortie de `k8s/deploy.sh` (étape 6/6) :
```
NAME                        READY   AGE
statefulset.apps/kafka      1/1     20m
statefulset.apps/postgres   1/1     20m

NAME                        READY   UP-TO-DATE   AVAILABLE   AGE
deployment.apps/dashboard   1/1     1            1           45s
deployment.apps/simulator   1/1     1            1           45s
deployment.apps/spark       1/1     1            1           45s

NAME                   STATUS     COMPLETIONS   DURATION   AGE
job.batch/consumer     Complete   1/1           4s         50s
job.batch/kafka-init   Complete   1/1           94s        2m35s
job.batch/producer     Complete   1/1           8s         60s

NAME                     TYPE        CLUSTER-IP       PORT(S)              AGE
service/dashboard        NodePort    10.106.36.201    8501:30851/TCP       45s
service/kafka            ClusterIP   10.104.251.165   29092/TCP            20m
service/kafka-headless   ClusterIP   None             29092/TCP,9093/TCP   20m
service/postgres         ClusterIP   10.101.54.12     5432/TCP             20m
service/spark-ui         ClusterIP   10.96.4.50       4040/TCP             45s

NAME                                      STATUS   CAPACITY   STORAGECLASS
persistentvolumeclaim/data-kafka-0        Bound    2Gi        standard
persistentvolumeclaim/data-postgres-0     Bound    1Gi        standard
persistentvolumeclaim/spark-checkpoints   Bound    1Gi        standard
```

## Fonctionnement dans le cluster
| Vérification | Résultat |
|---|---|
| Jobs de référentiels | `producer` : « 194 produits, 208 clients publiés » ; `consumer` : 194 et 208 lignes en base |
| Flux de commandes | 82 → 106 commandes et CA 156 692 $ → 223 549 $ en 30 s, sans intervention |
| Spark | micro-batches `orders` et `aggregates` toutes les ~10 s |
| Tableau de bord (exécuté dans son pod) | aucune exception, « ✅ Données à jour », 5 indicateurs remplis ; Service : `/_stcore/health` = ok, page HTTP 200 |

**Critère d'acceptation du cahier validé dans Kubernetes** : une commande simulée traverse Kafka et Spark, est persistée dans PostgreSQL et apparaît sur le tableau de bord sans intervention manuelle.

## Test « Pod indisponible » (section 13 du cahier)
Simulateur actif ; suppression successive de trois pods avec `kubectl delete pod`.

| Pod supprimé | Contrôleur | Recréé et prêt en | Effet observé |
|---|---|---|---|
| `spark-…` | Deployment | 36 s | reprise depuis le checkpoint (PVC), rattrapage du retard : CA 432 875 $ → 746 918 $ |
| `postgres-0` | StatefulSet | 11 s | données conservées (PVC) ; Spark, qui ne pouvait plus écrire, a redémarré une fois puis a repris |
| `kafka-0` | StatefulSet | 38 s | topics et messages conservés (PVC) ; flux repris : 224 → 332 commandes |

Contrôle d'intégrité ensuite (simulateur à 0 réplica, Job `check-pipeline`) :
```
Kafka : 362 messages, 0 doublon(s) ou hors contrat, 0 illisible(s), 314 paiements réussis uniques
Indicateur               Attendu (Kafka)      PostgreSQL
Chiffre d'affaires            1274562.22      1274562.22  OK
Quantités vendues                   1428            1428  OK
Commandes PAID                       314             314  OK
Commandes FAILED                      32              32  OK
Commandes CANCELLED                   16              16  OK
```
Aucune perte, aucun double comptage malgré la perte successive de Spark, PostgreSQL et Kafka.

## Difficultés rencontrées (et corrigées)
1. **Sonde de Kafka bloquée** : la première version vérifiait Kafka avec `kafka-topics.sh --list`. Ce client se reconnecte via l'adresse annoncée `kafka:29092`, c'est-à-dire le Service, qui n'achemine le trafic qu'aux pods déjà prêts : attente circulaire, pod jamais prêt. Remplacée par une sonde `tcpSocket` sur 29092.
2. **StatefulSet bloqué sur l'ancien pod** : après correction, un StatefulSet ne remplace pas un pod qui n'est pas prêt (mise à jour ordonnée). Il a fallu supprimer `kafka-0` pour qu'il soit recréé avec la nouvelle sonde, volume conservé.
3. **Variables injectées par Kubernetes** : un Service nommé `kafka` fait injecter `KAFKA_PORT=tcp://…` dans les pods, que l'image apache/kafka transformerait en propriété invalide. Évité avec `enableServiceLinks: false`.
4. **Images locales** : minikube a son propre moteur Docker ; `deploy.sh` y charge les images (`minikube image load`), et les manifestes utilisent `imagePullPolicy: Never`.

## Limites
- Un seul nœud et un seul broker (RF = 1) : Kubernetes recrée les pods, mais la perte du nœud entraînerait l'arrêt de la plateforme.
- Spark en `local[2]` dans un seul pod : pas de répartition sur plusieurs exécuteurs.
- Volumes `hostPath` de minikube : persistants tant que le cluster minikube n'est pas supprimé (`minikube delete` efface tout).
