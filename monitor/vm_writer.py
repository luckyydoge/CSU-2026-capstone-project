import os
import requests

VM_URL = os.environ.get(
    "VM_URL",
    "http://vm-storage-victoria-metrics-single-server.prometheus-system.svc:8428",
)

_worker_group_cache = None


def get_worker_group() -> str:
    global _worker_group_cache
    if _worker_group_cache is not None:
        return _worker_group_cache

    try:
        ns_file = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
        token_file = "/var/run/secrets/kubernetes.io/serviceaccount/token"
        ca_file = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        if os.path.exists(ns_file) and os.path.exists(token_file):
            ns = open(ns_file).read().strip()
            token = open(token_file).read().strip()
            host = os.environ.get("KUBERNETES_SERVICE_HOST",
                                  "kubernetes.default.svc.cluster.local")
            pod = os.environ.get("HOSTNAME", "")
            if pod:
                resp = requests.get(
                    f"https://{host}/api/v1/namespaces/{ns}/pods/{pod}",
                    headers={"Authorization": f"Bearer {token}"},
                    verify=ca_file,
                    timeout=5,
                )
                _worker_group_cache = (
                    resp.json()
                    .get("metadata", {})
                    .get("labels", {})
                    .get("ray.io/group", "unknown")
                )
                return _worker_group_cache
    except Exception:
        pass

    _worker_group_cache = os.environ.get("RAY_IO_GROUP", "unknown")
    return _worker_group_cache


def push_metric(name: str, value: float, tags: dict):
    tags_str = ",".join(f'{k}="{v}"' for k, v in sorted(tags.items()))
    line = f"{name}{{{tags_str}}} {value}"
    try:
        requests.post(f"{VM_URL}/api/v1/import/prometheus", data=line, timeout=2)
    except Exception:
        pass
