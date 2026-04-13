#! /bin/sh

# 创建kind
kind create cluster --image=kindest/node:v1.26.0

# 创建kuberay operator
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
# Install both CRDs and KubeRay operator v1.5.1.
helm install kuberay-operator kuberay/kuberay-operator --version 1.5.1

kubectl get pods

# 创建kuberay cluster
helm install raycluster kuberay/ray-cluster --version 1.6.0 --set image.tag=2.54.1-py312

