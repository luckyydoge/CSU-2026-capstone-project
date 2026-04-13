#! /bin/sh

kubectl port-forward service/raycluster-kuberay-head-svc 8265:8265 6379:6379 10001:10001
