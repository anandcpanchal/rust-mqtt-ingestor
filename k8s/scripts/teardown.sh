#!/bin/bash
echo "🛑 Tearing down Kubernetes Deployment..."

helm uninstall redpanda emqx prometheus grafana
kubectl delete -f k8s/apps/

# Optional: Delete PVCs to remove all data
kubectl delete pvc --all

echo "✅ Cleaned up."
