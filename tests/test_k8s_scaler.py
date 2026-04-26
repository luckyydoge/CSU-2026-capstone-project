from unittest.mock import MagicMock, patch

import pytest

from app.k8s_scaler import (
    RayClusterScaler,
    CRD_GROUP,
    CRD_VERSION,
    CRD_PLURAL,
)


@pytest.fixture
def mock_raycluster():
    return {
        "apiVersion": "ray.io/v1",
        "kind": "RayCluster",
        "metadata": {"name": "raycluster-kuberay"},
        "spec": {
            "workerGroupSpecs": [
                {
                    "groupName": "workergroup",
                    "replicas": 2,
                    "minReplicas": 1,
                    "maxReplicas": 5,
                }
            ]
        },
    }


@pytest.fixture
def scaler():
    with patch("app.k8s_scaler.config.load_kube_config"):
        s = RayClusterScaler()
        s.api = MagicMock()
        yield s


class TestScaleSet:
    def test_set_success(self, scaler, mock_raycluster):
        scaler.api.get_namespaced_custom_object.return_value = mock_raycluster
        result = scaler.scale_set("raycluster-kuberay", "workergroup", 3)
        assert result["success"] is True
        assert result["previous_replicas"] == 2
        assert result["current_replicas"] == 3
        expected_body = dict(mock_raycluster)
        expected_body["spec"]["workerGroupSpecs"][0]["replicas"] = 3
        scaler.api.replace_namespaced_custom_object.assert_called_once_with(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace="default",
            plural=CRD_PLURAL,
            name="raycluster-kuberay",
            body=expected_body,
        )

    def test_set_below_min(self, scaler, mock_raycluster):
        scaler.api.get_namespaced_custom_object.return_value = mock_raycluster
        result = scaler.scale_set("raycluster-kuberay", "workergroup", 0)
        assert result["success"] is False
        assert "below minReplicas" in result["message"]
        scaler.api.replace_namespaced_custom_object.assert_not_called()

    def test_set_above_max(self, scaler, mock_raycluster):
        scaler.api.get_namespaced_custom_object.return_value = mock_raycluster
        result = scaler.scale_set("raycluster-kuberay", "workergroup", 10)
        assert result["success"] is False
        assert "exceeds maxReplicas" in result["message"]
        scaler.api.replace_namespaced_custom_object.assert_not_called()

    def test_set_group_not_found(self, scaler, mock_raycluster):
        scaler.api.get_namespaced_custom_object.return_value = mock_raycluster
        result = scaler.scale_set("raycluster-kuberay", "nonexistent-group", 3)
        assert result["success"] is False
        assert "not found" in result["message"]
        scaler.api.replace_namespaced_custom_object.assert_not_called()

    def test_set_cluster_not_found(self, scaler):
        scaler.api.get_namespaced_custom_object.side_effect = Exception(
            "RayCluster not found"
        )
        result = scaler.scale_set("nonexistent-cluster", "workergroup", 3)
        assert result["success"] is False
        assert "Failed to get RayCluster" in result["message"]

    def test_set_replace_failure(self, scaler, mock_raycluster):
        scaler.api.get_namespaced_custom_object.return_value = mock_raycluster
        scaler.api.replace_namespaced_custom_object.side_effect = Exception("API error")
        result = scaler.scale_set("raycluster-kuberay", "workergroup", 3)
        assert result["success"] is False
        assert "Failed to patch replicas" in result["message"]


class TestScaleIncr:
    def test_incr_success(self, scaler, mock_raycluster):
        scaler.api.get_namespaced_custom_object.return_value = mock_raycluster
        result = scaler.scale_incr("raycluster-kuberay", "workergroup", 2)
        assert result["success"] is True
        assert result["previous_replicas"] == 2
        assert result["current_replicas"] == 4
        expected_body = dict(mock_raycluster)
        expected_body["spec"]["workerGroupSpecs"][0]["replicas"] = 4
        scaler.api.replace_namespaced_custom_object.assert_called_once_with(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace="default",
            plural=CRD_PLURAL,
            name="raycluster-kuberay",
            body=expected_body,
        )

    def test_incr_default_delta(self, scaler, mock_raycluster):
        scaler.api.get_namespaced_custom_object.return_value = mock_raycluster
        result = scaler.scale_incr("raycluster-kuberay", "workergroup")
        assert result["success"] is True
        assert result["current_replicas"] == 3

    def test_incr_exceeds_max(self, scaler, mock_raycluster):
        scaler.api.get_namespaced_custom_object.return_value = mock_raycluster
        result = scaler.scale_incr("raycluster-kuberay", "workergroup", 10)
        assert result["success"] is False
        assert "exceeds maxReplicas" in result["message"]
        scaler.api.replace_namespaced_custom_object.assert_not_called()

    def test_incr_group_not_found(self, scaler, mock_raycluster):
        scaler.api.get_namespaced_custom_object.return_value = mock_raycluster
        result = scaler.scale_incr("raycluster-kuberay", "nonexistent-group")
        assert result["success"] is False
        assert "not found" in result["message"]


class TestScaleDecr:
    def test_decr_success(self, scaler, mock_raycluster):
        scaler.api.get_namespaced_custom_object.return_value = mock_raycluster
        result = scaler.scale_decr("raycluster-kuberay", "workergroup", 1)
        assert result["success"] is True
        assert result["previous_replicas"] == 2
        assert result["current_replicas"] == 1
        expected_body = dict(mock_raycluster)
        expected_body["spec"]["workerGroupSpecs"][0]["replicas"] = 1
        scaler.api.replace_namespaced_custom_object.assert_called_once_with(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace="default",
            plural=CRD_PLURAL,
            name="raycluster-kuberay",
            body=expected_body,
        )

    def test_decr_default_delta(self, scaler, mock_raycluster):
        scaler.api.get_namespaced_custom_object.return_value = mock_raycluster
        result = scaler.scale_decr("raycluster-kuberay", "workergroup")
        assert result["success"] is True
        assert result["current_replicas"] == 1

    def test_decr_below_min(self, scaler, mock_raycluster):
        scaler.api.get_namespaced_custom_object.return_value = mock_raycluster
        result = scaler.scale_decr("raycluster-kuberay", "workergroup", 5)
        assert result["success"] is False
        assert "below minReplicas" in result["message"]
        scaler.api.replace_namespaced_custom_object.assert_not_called()

    def test_decr_group_not_found(self, scaler, mock_raycluster):
        scaler.api.get_namespaced_custom_object.return_value = mock_raycluster
        result = scaler.scale_decr("raycluster-kuberay", "nonexistent-group")
        assert result["success"] is False
        assert "not found" in result["message"]


class TestScalerInit:
    def test_default_init_with_kubeconfig(self):
        with patch("app.k8s_scaler.config.load_kube_config") as mock_load:
            s = RayClusterScaler()
            mock_load.assert_called_once()
            assert s.namespace == "default"

    def test_init_with_custom_namespace(self):
        with patch("app.k8s_scaler.config.load_kube_config"):
            s = RayClusterScaler(namespace="custom-ns")
            assert s.namespace == "custom-ns"

    def test_init_with_kubeconfig_path(self):
        with patch("app.k8s_scaler.config.load_kube_config") as mock_load:
            s = RayClusterScaler(kubeconfig="/custom/path/kubeconfig")
            mock_load.assert_called_once_with(config_file="/custom/path/kubeconfig")
            assert s.namespace == "default"
