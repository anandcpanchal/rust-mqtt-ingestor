$ErrorActionPreference = "Continue"

Write-Host "🛑 Tearing down Kubernetes Deployment..." -ForegroundColor Red

helm uninstall redpanda emqx prometheus grafana --ignore-not-found
kubectl delete -f k8s/apps/ --ignore-not-found

# Optional: Delete PVCs to remove all data
Write-Host "🧹 Cleaning up PVCs..." -ForegroundColor Yellow
kubectl delete pvc --all --ignore-not-found

Write-Host "`n✅ Cleaned up." -ForegroundColor Green
