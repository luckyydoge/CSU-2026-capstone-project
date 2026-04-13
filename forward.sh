#! /bin/sh

kubectl port-forward service/raycluster-kuberay-head-svc 8265:8265 6379:6379 10001:10001 --address 0.0.0.0 &

kubectl port-forward -n prometheus-system service/prometheus-kube-prometheus-prometheus 9090:9090 &

kubectl port-forward -n prometheus-system service/prometheus-grafana 3000:80 &
