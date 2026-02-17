#!/bin/bash
set -e

echo "🚀 Starting Local Kubernetes Deployment..."

# 1. Build Backend Image
echo "🔨 Building Backend Image..."
# Ensure we use Docker Desktop context
# kubectl config use-context docker-desktop
docker build -t ghcr.io/anandpanchal/poc-mqtt-backend:local .

# 2. Add Helm Repos
echo "📦 Adding Helm Repos..."
helm repo add emqx https://repos.emqx.io/charts
helm repo add redpanda https://charts.redpanda.com
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

# 3. Create DB Init ConfigMap
echo "💾 Creating Database Initialization ConfigMap..."
kubectl create configmap db-init --from-file=init.sql -o yaml --dry-run=client | kubectl apply -f -

# 4. Install Infrastructure (Helm)
echo "🏗️ Deploying Infrastructure..."

echo "   - Cert Manager (Required for Redpanda)"
helm repo add jetstack https://charts.jetstack.io
helm repo update
helm upgrade --install cert-manager jetstack/cert-manager \
  --namespace cert-manager --create-namespace \
  --version v1.13.3 \
  --set installCRDs=true --wait

echo "   - Monitoring Stack (Required for ServiceMonitor CRDs)"
# We use kube-prometheus-stack to get the operator and CRDs
helm upgrade --install prometheus prometheus-community/kube-prometheus-stack \
  --set prometheus.service.type=NodePort \
  --set grafana.service.type=NodePort \
  --set grafana.service.nodePort=30000 \
  --set grafana.adminPassword=admin \
  --set nodeExporter.enabled=false \
  --wait

echo "   - Redpanda"
helm upgrade --install redpanda redpanda/redpanda \
  --values k8s/helm-values/redpanda-values.yaml --wait

echo "   - Redpanda Topic Initialization"
kubectl delete job redpanda-init --ignore-not-found
kubectl apply -f k8s/apps/redpanda-init.yaml
kubectl wait --for=condition=complete job/redpanda-init --timeout=120s

echo "   - EMQX"
helm upgrade --install emqx emqx/emqx \
  --values k8s/helm-values/emqx-values.yaml --wait

# helm repo add timescaledb https://charts.timescale.com
echo "   - TimescaleDB (Using Custom Manifest)"
# helm upgrade --install timescaledb timescaledb/timescaledb-single ... (Removed due to chart issues)
kubectl apply -f k8s/apps/timescaledb.yaml
 kubectl wait --for=condition=ready pod -l app=timescaledb --timeout=120s

# 5.# Apply Applications
echo "Applying application manifests..."
kubectl apply -f k8s/apps/tempo.yaml
kubectl apply -f k8s/apps/secrets.yaml
kubectl apply -f k8s/apps/timescaledb.yaml
kubectl apply -f k8s/apps/vector.yaml
kubectl apply -f k8s/apps/backend.yaml

echo "✅ Deployment Complete!"
echo "   - EMQX Dashboard: http://localhost:18083 (admin/public)"
echo "   - Grafana: http://localhost:30000 (admin/admin)"
echo "   - Redpanda: exposed on NodePorts"
