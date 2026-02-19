$ErrorActionPreference = "Stop"

Write-Host "🚀 Starting Local Kubernetes Deployment..." -ForegroundColor Cyan

# 1. Build Backend Image
Write-Host "🔨 Building Backend Image..." -ForegroundColor Yellow
# Ensure we use Docker Desktop context
# kubectl config use-context docker-desktop
docker build -t ghcr.io/anandpanchal/poc-mqtt-backend:local .

# 2. Add Helm Repos
Write-Host "📦 Adding Helm Repos..." -ForegroundColor Yellow
helm repo add emqx https://repos.emqx.io/charts
helm repo add redpanda https://charts.redpanda.com
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

# 3. Create DB Init ConfigMap
Write-Host "💾 Creating Database Initialization ConfigMap..." -ForegroundColor Yellow
kubectl create configmap db-init --from-file=init.sql -o yaml --dry-run=client | kubectl apply -f -

# 4. Install Infrastructure (Helm)
Write-Host "🏗️ Deploying Infrastructure..." -ForegroundColor Yellow

Write-Host "   - Cert Manager (Required for Redpanda)"
helm repo add jetstack https://charts.jetstack.io
helm repo update
helm upgrade --install cert-manager jetstack/cert-manager `
  --namespace cert-manager --create-namespace `
  --version v1.13.3 `
  --set installCRDs=true --wait


Write-Host "   - Monitoring Stack (Required for ServiceMonitor CRDs)"
# We use kube-prometheus-stack to get the operator and CRDs
helm upgrade --install prometheus prometheus-community/kube-prometheus-stack `
  --values k8s/helm-values/monitoring-values.yaml

Write-Host "   - Redpanda"
helm upgrade --install redpanda redpanda/redpanda `
  --values k8s/helm-values/redpanda-values.yaml --wait

Write-Host "   - Redpanda Topic Initialization"
kubectl delete job redpanda-init --ignore-not-found
kubectl apply -f k8s/apps/redpanda-init.yaml
kubectl wait --for=condition=complete job/redpanda-init --timeout=120s

Write-Host "   - EMQX"
helm upgrade --install emqx emqx/emqx `
  --values k8s/helm-values/emqx-values.yaml --wait

# helm repo add timescaledb https://charts.timescale.com
Write-Host "   - TimescaleDB (Using Custom Manifest)"
# helm upgrade --install timescaledb timescaledb/timescaledb-single ... (Removed due to chart issues)
kubectl apply -f k8s/apps/timescaledb.yaml
kubectl wait --for=condition=ready pod -l app=timescaledb --timeout=120s

# 5. Apply Secrets & Apps
Write-Host "🚀 Deploying Applications..." -ForegroundColor Cyan
kubectl apply -f k8s/apps/metrics-monitors.yaml
kubectl apply -f k8s/apps/tempo.yaml
kubectl apply -f k8s/apps/secrets.yaml
kubectl apply -f k8s/apps/vector.yaml
kubectl apply -f k8s/apps/backend.yaml

Write-Host "`n✅ Deployment Complete!" -ForegroundColor Green
Write-Host "   - EMQX Dashboard: http://localhost:18083 (admin/public)"
Write-Host "   - Grafana: http://localhost:30000 (admin/admin)"
Write-Host "   - Redpanda: exposed on NodePorts"
Write-Host ""
Write-Host "Useful Port-Forward Commands (Run in separate terminals):" -ForegroundColor Cyan
Write-Host "   - TimescaleDB (Local Access): kubectl port-forward svc/timescaledb 5432:5432"
Write-Host "   - Tempo (OTLP gRPC):          kubectl port-forward svc/tempo 4317:4317"
