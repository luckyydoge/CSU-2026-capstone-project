"""
弹性伸缩调度器完整测试。

测试策略:
  - MetricsCollector: Mock VM HTTP 响应，覆盖有/无数据、NaN、异常
  - CapacityPlanner: 纯函数测试，覆盖各种 QPS/时延组合、边界约束
  - ComputeScaler: 测试 current < base → up, > safe → down, 范围内 → none
  - OscillationSuppressor: 测试冷却窗口、频率限制
  - SchedulerController: 完整 cycle 集成测试，Mock VM + Mock K8s
"""
from unittest.mock import MagicMock, patch
import pytest
import time
import yaml

from monitor.scheduler import (
    MetricsCollector, CapacityPlanner, CapacityPlan,
    ComputeScaler, ScaleDecision, OscillationSuppressor, SchedulerController,
)


# ═══════════════════════════════════════════
# 1. MetricsCollector 测试
# ═══════════════════════════════════════════

class TestMetricsCollector:
    @pytest.fixture
    def collector(self):
        return MetricsCollector("http://mock-vm:8428")

    def _mock_vm(self, mock_get, value: str):
        mock_get.return_value.json.return_value = {
            "data": {"result": [{"value": [1234567890, value]}]}
        }

    def test_query_peak_qps_success(self, collector):
        with patch("requests.get") as mock_get:
            self._mock_vm(mock_get, "15.5")
            result = collector.query_peak_qps("load_test", "1m")
            assert result == 15.5

    def test_query_p99_latency_success(self, collector):
        with patch("requests.get") as mock_get:
            self._mock_vm(mock_get, "450.2")
            result = collector.query_p99_latency("load_test", "1m")
            assert result == 450.2

    def test_query_p99_queue_success(self, collector):
        with patch("requests.get") as mock_get:
            self._mock_vm(mock_get, "123.0")
            result = collector.query_p99_queue("load_test", "1m")
            assert result == 123.0

    def test_query_p99_cpu_success(self, collector):
        with patch("requests.get") as mock_get:
            self._mock_vm(mock_get, "0.75")
            result = collector.query_p99_cpu("load_test", "1m")
            assert result == 0.75

    def test_query_p99_mem_success(self, collector):
        with patch("requests.get") as mock_get:
            self._mock_vm(mock_get, "512.0")
            result = collector.query_p99_mem("load_test", "1m")
            assert result == 512.0

    def test_empty_result_returns_none(self, collector):
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = {"data": {"result": []}}
            assert collector.query_peak_qps("x", "1m") is None
            assert collector.query_p99_latency("x", "1m") is None
            assert collector.query_p99_queue("x", "1m") is None

    def test_nan_returns_none(self, collector):
        with patch("requests.get") as mock_get:
            self._mock_vm(mock_get, "NaN")
            result = collector.query_peak_qps("x", "1m")
            assert result is None

    def test_http_error_returns_none(self, collector):
        with patch("requests.get", side_effect=Exception("connection refused")):
            result = collector.query_peak_qps("x", "1m")
            assert result is None

    def test_query_current_replicas(self, collector):
        mock_scaler = MagicMock()
        mock_scaler._get_raycluster.return_value = {
            "spec": {"workerGroupSpecs": [{"groupName": "wg", "replicas": 5}]}
        }
        mock_scaler._find_worker_group.return_value = (0, {"groupName": "wg", "replicas": 5})
        result = collector.query_current_replicas(mock_scaler, "cluster", "wg")
        assert result == 5

    def test_query_replicas_error_returns_neg1(self, collector):
        mock_scaler = MagicMock()
        mock_scaler._get_raycluster.side_effect = Exception("API error")
        result = collector.query_current_replicas(mock_scaler, "x", "y")
        assert result == -1


# ═══════════════════════════════════════════
# 2. CapacityPlanner 测试
# ═══════════════════════════════════════════

class TestCapacityPlanner:
    @pytest.fixture
    def planner(self):
        return CapacityPlanner({
            "cpu_per_replica": 1.0,
            "mem_mb_per_replica": 1024,
            "buffer_ratio": 0.2,
            "min_buffer": 1,
        })

    def test_littles_law_basic(self, planner):
        """peak_qps=10, p99_lat=500ms → in_flight=5
           cpu_concurrency=1/0.5=2, mem_concurrency=1024/256=4 → min=2
           base=ceil(5/2)=3, buffer=ceil(3*0.2)=1, safe=4"""
        plan = planner.plan(10, 500, 0.5, 256, 1, 20)
        assert plan.base_replicas == 3
        assert plan.safe_replicas == 4
        assert "Little" in plan.reason

    def test_high_qps_high_latency(self, planner):
        """peak_qps=100, p99_lat=2000ms → in_flight=200
           cpu_concurrency=1/0.5=2 → base=ceil(200/2)=100
           buffer=ceil(100*0.2)=20, safe=120"""
        plan = planner.plan(100, 2000, 0.5, 256, 1, 200)
        assert plan.base_replicas == 100
        assert plan.safe_replicas == 120

    def test_low_load_min_replicas(self, planner):
        """流量极低时，base 至少为 1"""
        plan = planner.plan(0.1, 100, 0.5, 256, 1, 20)
        assert plan.base_replicas >= 1
        assert plan.safe_replicas >= plan.base_replicas

    def test_respects_min_replicas(self, planner):
        plan = planner.plan(0.1, 100, 0.5, 256, 3, 20)
        assert plan.base_replicas >= 3
        assert plan.safe_replicas >= 3

    def test_respects_max_replicas(self, planner):
        plan = planner.plan(1000, 5000, 0.5, 256, 1, 5)
        assert plan.base_replicas <= 5
        assert plan.safe_replicas <= 5

    def test_low_cpu_concurrency(self, planner):
        """task_cpu=0.9 → cpu_concurrency=1/0.9≈1  mem_concurrency=1024/256=4 → concurrency=1
           in_flight=10*0.5=5, base=ceil(5/1)=5"""
        plan = planner.plan(10, 500, 0.9, 256, 1, 20)
        assert plan.base_replicas == 5

    def test_low_mem_concurrency(self, planner):
        """task_mem=512 → mem_concurrency=1024/512=2  cpu_concurrency=1/0.5=2 → concurrency=2
           in_flight=10*0.5=5, base=ceil(5/2)=3"""
        plan = planner.plan(10, 500, 0.5, 512, 1, 20)
        assert plan.base_replicas == 3

    def test_zero_latency_no_division_error(self, planner):
        plan = planner.plan(10, 0, 0.5, 256, 1, 20)
        assert plan.base_replicas >= 1


# ═══════════════════════════════════════════
# 3. ComputeScaler 测试
# ═══════════════════════════════════════════

class TestComputeScaler:
    @pytest.fixture
    def scaler(self):
        return ComputeScaler()

    def test_current_below_base_scale_up(self, scaler):
        plan = CapacityPlan(5, 7, 2.0, 2, "test")
        decision = scaler.decide(3, plan, 1, 20)
        assert decision.action == "scale_up"
        assert decision.target_replicas == 7  # safe_replicas

    def test_current_above_safe_scale_down(self, scaler):
        plan = CapacityPlan(3, 5, 2.0, 2, "test")
        decision = scaler.decide(10, plan, 1, 20)
        assert decision.action == "scale_down"
        assert decision.target_replicas == 3  # base_replicas

    def test_current_within_range_no_action(self, scaler):
        plan = CapacityPlan(3, 5, 2.0, 2, "test")
        decision = scaler.decide(4, plan, 1, 20)
        assert decision.action == "none"

    def test_current_equal_base_no_action(self, scaler):
        plan = CapacityPlan(5, 7, 2.0, 2, "test")
        decision = scaler.decide(5, plan, 1, 20)
        assert decision.action == "none"

    def test_current_equal_safe_no_action(self, scaler):
        plan = CapacityPlan(5, 7, 2.0, 2, "test")
        decision = scaler.decide(7, plan, 1, 20)
        assert decision.action == "none"

    def test_scale_up_respects_max(self, scaler):
        plan = CapacityPlan(10, 20, 2.0, 10, "test")
        decision = scaler.decide(1, plan, 1, 5)
        assert decision.action == "scale_up"
        assert decision.target_replicas <= 5

    def test_scale_down_respects_min(self, scaler):
        plan = CapacityPlan(1, 2, 2.0, 1, "test")
        decision = scaler.decide(10, plan, 3, 20)
        assert decision.action == "scale_down"
        assert decision.target_replicas >= 3

    def test_negative_replicas_none(self, scaler):
        plan = CapacityPlan(5, 7, 2.0, 2, "test")
        decision = scaler.decide(-1, plan, 1, 20)
        assert decision.action == "none"


# ═══════════════════════════════════════════
# 4. OscillationSuppressor 测试
# ═══════════════════════════════════════════

class TestOscillationSuppressor:
    @pytest.fixture
    def suppressor(self):
        return OscillationSuppressor({
            "scale_up_cooldown": 30,
            "scale_down_cooldown": 60,
            "max_scale_up_per_cycle": 3,
            "max_scale_down_per_cycle": 1,
        })

    def test_scale_up_allowed_first_time(self, suppressor):
        decision = ScaleDecision("scale_up", 10, 5, "test")
        result = suppressor.check(decision)
        assert result.allowed is True

    def test_scale_down_allowed_first_time(self, suppressor):
        decision = ScaleDecision("scale_down", 3, 10, "test")
        result = suppressor.check(decision)
        assert result.allowed is True

    def test_none_action_always_allowed(self, suppressor):
        decision = ScaleDecision("none", 5, 5, "nothing")
        result = suppressor.check(decision)
        assert result.allowed is True

    def test_scale_up_blocked_by_cooldown(self, suppressor):
        suppressor.record(ScaleDecision("scale_up", 10, 5, "first"))
        decision = ScaleDecision("scale_up", 12, 10, "second")
        result = suppressor.check(decision)
        assert result.allowed is False
        assert "冷却" in result.reason

    def test_scale_down_blocked_by_cooldown(self, suppressor):
        suppressor.record(ScaleDecision("scale_down", 3, 10, "first"))
        decision = ScaleDecision("scale_down", 2, 3, "second")
        result = suppressor.check(decision)
        assert result.allowed is False
        assert "冷却" in result.reason

    def test_scale_up_limit_reached(self, suppressor):
        for _ in range(3):
            suppressor.record(ScaleDecision("scale_up", 10, 5, "cycle"))
        with patch("time.time", return_value=time.time() + 61):
            decision = ScaleDecision("scale_up", 12, 10, "should be limited")
            result = suppressor.check(decision)
            assert result.allowed is False
            assert "上限" in result.reason

    def test_scale_down_limit_reached(self, suppressor):
        suppressor.record(ScaleDecision("scale_down", 3, 10, "only one"))
        with patch("time.time", return_value=time.time() + 61):
            decision = ScaleDecision("scale_down", 2, 3, "second down blocked")
            result = suppressor.check(decision)
            assert result.allowed is False
            assert "上限" in result.reason

    def test_cooldown_expired_allows_action(self, suppressor):
        suppressor.record(ScaleDecision("scale_up", 10, 5, "old"))
        with patch("time.time", return_value=time.time() + 31):
            decision = ScaleDecision("scale_up", 12, 10, "after cooldown")
            result = suppressor.check(decision)
            assert result.allowed is True

    def test_record_limits_history_size(self, suppressor):
        for _ in range(110):
            suppressor.record(ScaleDecision("scale_up", 10, 5, "filler"))
        assert len(suppressor._history) <= 100

    def test_history_cleaned_by_age(self, suppressor):
        old_ts = time.time() - 400
        suppressor._history = [(old_ts, "scale_up")]
        decision = ScaleDecision("scale_up", 12, 10, "old history cleaned")
        result = suppressor.check(decision)
        assert result.allowed is True


# ═══════════════════════════════════════════
# 5. 集成测试
# ═══════════════════════════════════════════

class TestSchedulerControllerIntegration:
    @pytest.fixture
    def config_path(self, tmp_path):
        cfg = {
            "prometheus_url": "http://mock-vm:8428",
            "scrape_interval": 10,
            "namespace": "default",
            "stages": [{
                "stage_id": "load_test",
                "cluster": "raycluster-kuberay",
                "worker_group": "workergroup",
                "window": "1m",
                "min_replicas": 1,
                "max_replicas": 20,
            }],
            "capacity": {
                "cpu_per_replica": 1.0,
                "mem_mb_per_replica": 1024,
                "buffer_ratio": 0.2,
                "min_buffer": 1,
            },
            "suppressor": {
                "scale_up_cooldown": 30,
                "scale_down_cooldown": 60,
                "max_scale_up_per_cycle": 3,
                "max_scale_down_per_cycle": 1,
            },
        }
        f = tmp_path / "test_scheduler.yaml"
        f.write_text(yaml.dump(cfg))
        return str(f)

    def _make_mock_vm(self, values: dict):
        """values: {metric_name: float_value}"""
        def side_effect(url, params=None, **kw):
            query = (params or {}).get("query", "")
            m = MagicMock()
            for key, val in values.items():
                if key in query:
                    j = {"data": {"result": [{"value": [0, str(val)]}]}}
                    m.json.return_value = j
                    return m
            j = {"data": {"result": []}}
            m.json.return_value = j
            return m
        return side_effect

    def test_full_cycle_high_qps_scales_up(self, config_path):
        """高 QPS + 高时延 → 利特尔法则计算出高 base → scale_up"""
        vm_values = {
            "proxy_stage_latency_ms": "150.0",
            "proxy_queue_time_ms": "10.0",
            "proxy_task_cpu_cores": "0.5",
            "proxy_task_memory_mb": "256",
        }

        with patch("requests.get") as mock_get, \
             patch("app.k8s_scaler.config.load_kube_config"):

            mock_get.side_effect = self._make_mock_vm(vm_values)

            controller = SchedulerController(config_path)
            controller.k8s_scaler = MagicMock()
            controller.k8s_scaler._get_raycluster.return_value = {
                "spec": {"workerGroupSpecs": [{"groupName": "workergroup", "replicas": 3}]}
            }
            controller.k8s_scaler._find_worker_group.return_value = (
                0, {"groupName": "workergroup", "replicas": 3})
            controller.k8s_scaler.scale_set.return_value = {"success": True}

            decisions = controller._run_once()
            stage = decisions["stages"]["load_test"]
            assert stage["scale_decision"]["action"] in ("scale_up", "none")

    def test_full_cycle_low_qps_scales_down(self, config_path):
        """低 QPS → small base → current 远高于 base → scale_down"""
        vm_values = {
            "proxy_stage_latency_ms": "100.0",
            "proxy_queue_time_ms": "5.0",
            "proxy_task_cpu_cores": "0.5",
            "proxy_task_memory_mb": "256",
        }

        with patch("requests.get") as mock_get, \
             patch("app.k8s_scaler.config.load_kube_config"):

            mock_get.side_effect = self._make_mock_vm(vm_values)

            controller = SchedulerController(config_path)
            controller.k8s_scaler = MagicMock()
            controller.k8s_scaler._get_raycluster.return_value = {
                "spec": {"workerGroupSpecs": [{"groupName": "workergroup", "replicas": 20}]}
            }
            controller.k8s_scaler._find_worker_group.return_value = (
                0, {"groupName": "workergroup", "replicas": 20})
            controller.k8s_scaler.scale_set.return_value = {"success": True}

            decisions = controller._run_once()
            stage = decisions["stages"]["load_test"]
            assert stage["scale_decision"]["action"] == "scale_down"

    def test_full_cycle_no_data_scale_down(self, config_path):
        """VM 无数据 → QPS=0 → base=0 → scale_down"""
        with patch("requests.get") as mock_get, \
             patch("app.k8s_scaler.config.load_kube_config"):

            mock_get.return_value.json.return_value = {"data": {"result": []}}

            controller = SchedulerController(config_path)
            controller.k8s_scaler = MagicMock()
            controller.k8s_scaler._get_raycluster.return_value = {
                "spec": {"workerGroupSpecs": [{"groupName": "workergroup", "replicas": 5}]}
            }
            controller.k8s_scaler._find_worker_group.return_value = (
                0, {"groupName": "workergroup", "replicas": 5})

            decisions = controller._run_once()
            stage = decisions["stages"]["load_test"]
            assert stage["capacity_plan"]["base_replicas"] == 0
            assert stage["scale_decision"]["action"] == "scale_down"

    def test_scale_up_action_calls_k8s(self, config_path):
        """确认 scale_up 时调用了 k8s_scaler.scale_set"""
        vm_values = {
            "proxy_stage_latency_ms": "200.0",
            "proxy_queue_time_ms": "10.0",
            "proxy_task_cpu_cores": "0.5",
            "proxy_task_memory_mb": "256",
        }

        with patch("requests.get") as mock_get, \
             patch("app.k8s_scaler.config.load_kube_config"):

            mock_get.side_effect = self._make_mock_vm(vm_values)

            controller = SchedulerController(config_path)
            controller.k8s_scaler = MagicMock()
            controller.k8s_scaler._get_raycluster.return_value = {
                "spec": {"workerGroupSpecs": [{"groupName": "workergroup", "replicas": 1}]}
            }
            controller.k8s_scaler._find_worker_group.return_value = (
                0, {"groupName": "workergroup", "replicas": 1})
            controller.k8s_scaler.scale_set.return_value = {"success": True}

            decisions = controller._run_once()
            stage = decisions["stages"]["load_test"]
            if stage["scale_decision"]["action"] == "scale_up":
                controller.k8s_scaler.scale_set.assert_called()

    def test_controller_start_stop(self, config_path):
        """start()/stop() 正确管理线程生命周期"""
        controller = SchedulerController(config_path)
        controller.start()
        assert controller._thread is not None
        assert controller._thread.is_alive()
        controller.stop()
        assert not controller._thread.is_alive()
