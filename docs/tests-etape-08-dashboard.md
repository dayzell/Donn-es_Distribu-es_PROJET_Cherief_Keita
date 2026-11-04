# Étape 08 — Tests du tableau de bord (et PostgreSQL indisponible)

Tests réalisés le 3 novembre 2026 sur la stack Docker Compose, simulateur actif, Spark en fonctionnement continu depuis la veille (environ 7 000 commandes en base).

L'application Streamlit a été exécutée avec l'outil de test intégré de Streamlit (`streamlit.testing.v1.AppTest`), dans le conteneur `retailpulse-dashboard`. Il exécute le script comme le ferait un navigateur et permet de lire les indicateurs, bandeaux et tableaux affichés.

## Synthèse
| # | Scénario | Résultat attendu | Résultat obtenu |
|---|---|---|---|
| 1 | Démarrage du conteneur | page servie, conteneur sain | **OK** : `Up (healthy)`, `/_stcore/health` = `ok`, page HTTP 200 |
| 2 | Exécution de l'application | aucune exception, 5 indicateurs, 5 sections | **OK** : aucune exception ni erreur ; graphiques et 3 tableaux remplis |
| 3 | Indicateurs = PostgreSQL (pas de recalcul) | CA et articles égaux aux requêtes SQL directes | **OK** : CA 47 005 432,38 $ et 29 612 articles des deux côtés |
| 4 | Critère d'acceptation : une commande simulée devient visible sans intervention | nouvelles commandes et CA en hausse entre deux lectures | **OK** : à 30 s d'intervalle, commandes 7 097 → 7 120, CA 47 600 906,65 $ → 47 773 808,09 $, dernière commande passée de `ORD-58f34a18…` (11:00:19) à `ORD-e7becbec…` (11:00:49), statut « ✅ Payée » |
| 5 | Flux arrêté | bandeau d'alerte après 60 s | **OK** : « ⚠️ Données figées : dernier agrégat écrit il y a 73 s » |
| 6 | PostgreSQL arrêté | message d'erreur, pas de plantage, retour automatique | **OK** : « Connexion à PostgreSQL impossible… Nouvel essai dans 10 s » ; affichage normal après redémarrage de PostgreSQL, sans action sur le tableau de bord |

Remarque test 3 : le nombre de commandes différait de 7 entre la page (7 010) et la requête SQL (7 017), lancée quelques secondes après : le simulateur était actif. Le CA et les articles, qui ne dépendent que des agrégats, étaient identiques.

## Scénario « PostgreSQL indisponible » (section 13 du cahier)
Manipulation : simulateur actif, `docker compose stop postgres` pendant 60 s, puis `docker compose start postgres`.

| Composant | Comportement observé |
|---|---|
| Tableau de bord | message d'erreur clair, nouvel essai toutes les 10 s ; reprend seul au retour de PostgreSQL |
| Simulateur | non affecté : il ne lit PostgreSQL qu'au démarrage et continue de publier dans Kafka |
| Kafka | conserve les événements produits pendant la coupure |
| Spark | la requête qui écrit dans PostgreSQL échoue (`PSQLException: The connection attempt failed`) et le processus Spark s'arrête ; Docker le relance automatiquement (`restart: unless-stopped`, 3 redémarrages pendant la coupure) ; au retour de PostgreSQL, Spark reprend depuis son checkpoint et traite le retard accumulé |
| Intégrité | après arrêt du simulateur, `tests/check_pipeline.py` : **OK** sur toutes les lignes (CA 49 104 226,71 $, 6 167 PAID, 767 FAILED, 378 CANCELLED) — aucune perte, aucun double comptage |

### Analyse
- La garantie vient de la combinaison : Kafka conserve les événements, le checkpoint Spark mémorise les offsets réellement écrits, et l'upsert rend sans effet la réécriture d'un micro-batch.
- Limite : Spark ne gère pas la coupure lui-même (pas de nouvelle tentative dans le code), il s'appuie sur le redémarrage du conteneur. En Kubernetes, le Deployment jouera le même rôle. Une amélioration possible serait d'ajouter des tentatives avec attente dans les fonctions d'écriture `foreachBatch`.
- Pendant la coupure, le tableau de bord ne montre plus rien : il n'a pas de cache. Choix assumé : mieux vaut un message d'erreur qu'un chiffre périmé présenté comme à jour.
