# Étape 06 — Tests de Spark Structured Streaming

Tests réalisés le 2 novembre 2026 sur la stack Docker Compose (Kafka 4.0, Spark 3.5.3 en `local[2]`, PostgreSQL 16). Fenêtre 5 minutes, watermark 10 minutes, trigger 10 secondes.

Outil de contrôle : `tests/check_pipeline.py`. Il relit tout `retail-order-events`, applique les règles métier indépendamment de Spark (seul `PAYMENT_COMPLETED` compte, un `event_id` n'est compté qu'une fois) et compare avec PostgreSQL.

## Synthèse
| # | Scénario (section 13 du cahier) | Manipulation | Résultat attendu | Résultat obtenu |
|---|---|---|---|---|
| 1 | Traitement de l'historique | Démarrer Spark avec 499 commandes déjà dans Kafka | CA et statuts identiques à Kafka | **OK** : 423 PAID, 51 FAILED, 25 CANCELLED ; CA 3 565 415,21 $ des deux côtés ; 71 agrégats, 24 catégories |
| 2 | Reprise Spark | Redémarrer le conteneur Spark | Pas de retraitement, pas de double comptage | **OK** : aucun micro-batch relancé sur les anciennes données ; contrôle identique après redémarrage |
| 3 | Flux nominal | Simulateur actif 60 s | Les agrégats suivent le flux | **OK** : un micro-batch toutes les ~10 s ; après arrêt, CA 3 909 747,60 $ des deux côtés |
| 4 | Doublon | 10 commandes, chaque paiement publié deux fois (même `event_id`) | Chaque paiement compté une fois | **OK** : 10 doublons détectés dans Kafka ; CA 3 953 203,19 $ des deux côtés |
| 5 | Paiement échoué | Paiements `PAYMENT_FAILED` du simulateur (10 %) | Aucun effet sur le CA | **OK** : statut `FAILED` dans `orders`, CA identique au calcul « PAYMENT_COMPLETED seulement » |
| 6 | Messages invalides | Texte non JSON, champs manquants, type `REFUND`, produit `99999` | Rejetés et tracés, CA inchangé | **OK** : 4 lignes dans `invalid_events` avec la bonne raison ; CA inchangé |
| 7 | Événement tardif | Deux paiements de produit 1 : retard 3 min (99,90 $) et retard 30 min (69,93 $) | Retard < watermark compté, retard > watermark ignoré | **OK** : le CA augmente de 99,90 $ exactement |

## Détail des messages invalides (test 6)
| Message injecté | Raison enregistrée |
|---|---|
| `ceci nest pas du json` | `json_invalide_ou_champ_manquant` |
| `{"event_id":"x1","event_type":"PAYMENT_COMPLETED"}` | `json_invalide_ou_champ_manquant` |
| événement complet de type `REFUND` | `type_inattendu` |
| `PAYMENT_COMPLETED` avec `product_id` 99999 | `produit_inconnu` |

Pour le produit inconnu, la commande reste enregistrée dans `orders` (le client existe), mais sa ligne n'entre pas dans le CA par catégorie : la catégorie est impossible à déterminer.

## Analyse
- La déduplication et l'upsert sont complémentaires : la déduplication évite de compter deux fois un événement rejoué, l'upsert évite de dupliquer une ligne d'agrégat quand Spark réécrit un micro-batch après une reprise.
- Le watermark fait un compromis : 10 minutes de retard toléré, contre un état mémoire borné. Un événement plus ancien est perdu pour les agrégats ; il reste visible dans Kafka et dans `orders`.
- Les tests 6 et 7 ont volontairement introduit des écarts : `check_pipeline.py` signale désormais un écart sur ce jeu de données (produit inconnu et paiement tardif comptés par le script, écartés par Spark). Repartir de zéro avec `docker compose down -v`.
- Premier micro-batch : environ 14 à 24 s pour 499 commandes en attente (avertissement « falling behind » de Spark, normal au rattrapage). Ensuite, les micro-batches suivent le trigger de 10 s. Mesures de débit et de latence détaillées : étape 10.
