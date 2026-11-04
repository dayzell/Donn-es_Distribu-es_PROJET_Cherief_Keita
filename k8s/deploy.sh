#!/usr/bin/env bash
# Déploie RetailPulse dans le cluster Kubernetes courant (namespace retailpulse), dans l'ordre :
# configuration -> Kafka + PostgreSQL -> Jobs (topics, collecte, chargement) -> applications.
# Usage, depuis la racine du projet : bash k8s/deploy.sh
# SKIP_BUILD=1 : ne pas reconstruire les images ni les recharger dans minikube (déjà fait).
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1   # Git Bash sous Windows : ne pas réécrire les chemins
NS=retailpulse

kubectl get nodes --request-timeout=10s > /dev/null 2>&1 || {
  echo "Cluster Kubernetes injoignable : démarrer le cluster (minikube start), puis 'kubectl get nodes'." >&2
  exit 1
}
[ -f .env ] || { echo "Fichier .env manquant : cp .env.example .env" >&2; exit 1; }
env_value() { grep -E "^$1=" .env | head -1 | cut -d= -f2- | tr -d '\r'; }

IMAGES="retailpulse-producer:1.0 retailpulse-consumer:1.0 retailpulse-simulator:1.0 retailpulse-spark:1.0 retailpulse-dashboard:1.0"
echo "== 1/6 Images versionnées (retailpulse-<service>:1.0)"
if [ "${SKIP_BUILD:-0}" != "1" ]; then
  docker compose build producer consumer simulator spark dashboard
fi
# minikube a son propre moteur Docker : les images construites sur la machine doivent y être chargées
# (imagePullPolicy: Never dans les manifestes). Celle de Spark (~1,9 Go) prend quelques minutes.
if [ "${SKIP_BUILD:-0}" != "1" ] && [ "$(kubectl config current-context)" = "minikube" ]; then
  for image in $IMAGES; do
    echo "   chargement dans minikube : $image"
    minikube image load "$image"
  done
fi

echo "== 2/6 Namespace, configuration et secret"
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/config/configmap.yaml
# Le secret est généré depuis .env : aucun mot de passe n'est écrit dans les manifestes versionnés.
kubectl -n "$NS" create secret generic retailpulse-db \
  --from-literal=POSTGRES_USER="$(env_value POSTGRES_USER)" \
  --from-literal=POSTGRES_PASSWORD="$(env_value POSTGRES_PASSWORD)" \
  --from-literal=POSTGRES_DB="$(env_value POSTGRES_DB)" \
  --dry-run=client -o yaml | kubectl apply -f -
# Les scripts existants sont réutilisés tels quels via des ConfigMaps.
kubectl -n "$NS" create configmap kafka-scripts --from-file=kafka/create-topics.sh \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" create configmap postgres-init --from-file=postgres/init/ \
  --dry-run=client -o yaml | kubectl apply -f -

echo "== 3/6 Kafka et PostgreSQL (StatefulSets)"
kubectl apply -f k8s/kafka/kafka.yaml -f k8s/postgres/postgres.yaml
kubectl -n "$NS" rollout status statefulset/postgres --timeout=300s
kubectl -n "$NS" rollout status statefulset/kafka --timeout=300s

echo "== 4/6 Jobs : topics, collecte DummyJSON, chargement des référentiels"
run_job() {
  kubectl -n "$NS" delete job "$1" --ignore-not-found
  kubectl apply -f "$2"
  kubectl -n "$NS" wait --for=condition=complete "job/$1" --timeout=300s
}
run_job kafka-init k8s/kafka/topics-job.yaml
run_job producer k8s/jobs/producer-job.yaml
run_job consumer k8s/jobs/consumer-job.yaml

echo "== 5/6 Applications : spark, simulator, dashboard"
kubectl apply -f k8s/apps/
for app in spark simulator dashboard; do
  kubectl -n "$NS" rollout status "deployment/$app" --timeout=300s
done

echo "== 6/6 État du déploiement"
kubectl -n "$NS" get statefulsets,deployments,jobs,pods,services,pvc
echo
if [ "$(kubectl config current-context)" = "minikube" ]; then
  echo "Tableau de bord : minikube service dashboard -n $NS   (ouvre le navigateur ; garder le terminal ouvert)"
else
  echo "Tableau de bord : http://localhost:30851"
fi
echo "Spark UI        : kubectl -n $NS port-forward svc/spark-ui 4040:4040   puis http://localhost:4040"
