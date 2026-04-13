from typing import Optional

from kubernetes import client, config

CRD_GROUP = "ray.io"
CRD_VERSION = "v1"
CRD_PLURAL = "rayclusters"


class RayClusterScaler:
    def __init__(self, namespace: str = "default", kubeconfig: Optional[str] = None):
        if kubeconfig:
            config.load_kube_config(config_file=kubeconfig)
        else:
            try:
                config.load_incluster_config()
            except config.ConfigException:
                config.load_kube_config()
        self.api = client.CustomObjectsApi()
        self.namespace = namespace

    def _get_raycluster(self, cluster_name: str) -> dict:
        return self.api.get_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=self.namespace,
            plural=CRD_PLURAL,
            name=cluster_name,
        )

    def _find_worker_group(self, raycluster: dict, group_name: str) -> tuple:
        specs = raycluster.get("spec", {}).get("workerGroupSpecs", [])
        for i, spec in enumerate(specs):
            if spec.get("groupName") == group_name:
                return i, spec
        return -1, None

    def _patch_replicas(
        self, cluster_name: str, group_index: int, replicas: int
    ) -> dict:
        patch = [
            {
                "op": "replace",
                "path": f"/spec/workerGroupSpecs/{group_index}/replicas",
                "value": replicas,
            }
        ]
        return self.api.patch_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=self.namespace,
            plural=CRD_PLURAL,
            name=cluster_name,
            body=patch,
        )

    def scale_set(self, cluster_name: str, group_name: str, replicas: int) -> dict:
        try:
            rc = self._get_raycluster(cluster_name)
        except Exception as e:
            return {"success": False, "message": f"Failed to get RayCluster: {e}"}

        group_index, spec = self._find_worker_group(rc, group_name)
        if spec is None or group_index < 0:
            return {
                "success": False,
                "message": f"Worker group '{group_name}' not found in cluster '{cluster_name}'",
            }

        min_replicas = spec.get("minReplicas", 0)
        max_replicas = spec.get("maxReplicas", 2147483647)
        if replicas < min_replicas:
            return {
                "success": False,
                "message": f"Replicas {replicas} is below minReplicas {min_replicas}",
            }
        if replicas > max_replicas:
            return {
                "success": False,
                "message": f"Replicas {replicas} exceeds maxReplicas {max_replicas}",
            }

        old_replicas = spec.get("replicas", 0)
        try:
            self._patch_replicas(cluster_name, group_index, replicas)
        except Exception as e:
            return {"success": False, "message": f"Failed to patch replicas: {e}"}

        return {
            "success": True,
            "message": f"Scaled worker group '{group_name}' from {old_replicas} to {replicas}",
            "previous_replicas": old_replicas,
            "current_replicas": replicas,
        }

    def scale_incr(self, cluster_name: str, group_name: str, delta: int = 1) -> dict:
        try:
            rc = self._get_raycluster(cluster_name)
        except Exception as e:
            return {"success": False, "message": f"Failed to get RayCluster: {e}"}

        group_index, spec = self._find_worker_group(rc, group_name)
        if spec is None or group_index < 0:
            return {
                "success": False,
                "message": f"Worker group '{group_name}' not found in cluster '{cluster_name}'",
            }

        current = spec.get("replicas", 0)
        max_replicas = spec.get("maxReplicas", 2147483647)
        new_replicas = current + delta
        if new_replicas > max_replicas:
            return {
                "success": False,
                "message": (
                    f"Cannot increase by {delta}: "
                    f"{new_replicas} exceeds maxReplicas {max_replicas}"
                ),
            }

        try:
            self._patch_replicas(cluster_name, group_index, new_replicas)
        except Exception as e:
            return {"success": False, "message": f"Failed to patch replicas: {e}"}

        return {
            "success": True,
            "message": (
                f"Increased worker group '{group_name}' "
                f"from {current} to {new_replicas} (delta={delta})"
            ),
            "previous_replicas": current,
            "current_replicas": new_replicas,
        }

    def scale_decr(self, cluster_name: str, group_name: str, delta: int = 1) -> dict:
        try:
            rc = self._get_raycluster(cluster_name)
        except Exception as e:
            return {"success": False, "message": f"Failed to get RayCluster: {e}"}

        group_index, spec = self._find_worker_group(rc, group_name)
        if spec is None or group_index < 0:
            return {
                "success": False,
                "message": f"Worker group '{group_name}' not found in cluster '{cluster_name}'",
            }

        current = spec.get("replicas", 0)
        min_replicas = spec.get("minReplicas", 0)
        new_replicas = current - delta
        if new_replicas < min_replicas:
            return {
                "success": False,
                "message": (
                    f"Cannot decrease by {delta}: "
                    f"{new_replicas} is below minReplicas {min_replicas}"
                ),
            }

        try:
            self._patch_replicas(cluster_name, group_index, new_replicas)
        except Exception as e:
            return {"success": False, "message": f"Failed to patch replicas: {e}"}

        return {
            "success": True,
            "message": (
                f"Decreased worker group '{group_name}' "
                f"from {current} to {new_replicas} (delta={delta})"
            ),
            "previous_replicas": current,
            "current_replicas": new_replicas,
        }
