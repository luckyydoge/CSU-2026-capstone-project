"""
弹性伸缩调度器

基于利特尔法则（Little's Law）计算目标副本数，驱动 K8s RayCluster 扩缩容。

文件结构:
  MetricsCollector      — VM PromQL 指标采集
  CapacityPlan          — 容量规划结果数据类
  CapacityPlanner       — 利特尔法则稳态计算
  ScaleDecision         — 扩缩容决策数据类
  ComputeScaler         — 计算驱动的扩缩决策
  SuppressResult        — 振荡抑制结果数据类
  OscillationSuppressor — 冷却窗口 + 频率限制
  SchedulerController   — 主控制循环
"""
import math
import re
import time
import threading
import logging
import yaml
import requests
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from app.k8s_scaler import RayClusterScaler

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s"
)

# ─────────────────────────────────────────────
# 1. 指标采集器
# ─────────────────────────────────────────────


class MetricsCollector:
    """从 VictoriaMetrics 采集实时指标。

    所有查询均通过 /api/v1/query 使用 instant query。
    返回 None 表示无数据或异常。
    """

    def __init__(self, vm_url: str):
        self.vm_url = vm_url.rstrip("/")

    def query_peak_qps(self, stage_id: str, qps_window: str) -> Optional[float]:
        """窗口内峰值 QPS（平滑平均后的最大值）"""
        q = (
            f"max by (stage_id) ("
            f"avg_over_time("
            f"(sum by (stage_id) (count_over_time("
            f"proxy_stage_latency_ms{{stage_id=\"{stage_id}\"}}[30s])) / 30"
            f")[{qps_window}:])"
            f")"
        )
        ret = self._query_float(q)
        return ret

    def query_p99_latency(self, stage_id: str, window: str) -> Optional[float]:
        """P99 任务执行时延（ms）"""
        q = f"quantile_over_time(0.99, proxy_stage_latency_ms{{stage_id=\"{stage_id}\"}}[{window}])"
        return self._query_float(q)

    def query_p99_queue(self, stage_id: str, window: str) -> Optional[float]:
        """P99 排队时延（ms）"""
        q = f"quantile_over_time(0.99, proxy_queue_time_ms{{stage_id=\"{stage_id}\"}}[{window}])"
        return self._query_float(q)

    def query_p99_cpu(self, stage_id: str, window: str) -> Optional[float]:
        """P99 任务 CPU 核数"""
        q = f"quantile_over_time(0.99, proxy_task_cpu_cores{{stage_id=\"{stage_id}\"}}[{window}])"
        return self._query_float(q)

    def query_p99_mem(self, stage_id: str, window: str) -> Optional[float]:
        """P99 任务内存（MB）"""
        q = f"quantile_over_time(0.99, proxy_task_memory_mb{{stage_id=\"{stage_id}\"}}[{window}])"
        return self._query_float(q)

    def query_current_replicas(self, scaler: RayClusterScaler,
                               cluster: str, group: str) -> int:
        """从 K8s RayCluster CRD 查询当前副本数"""
        try:
            rc = scaler._get_raycluster(cluster)
            _, spec = scaler._find_worker_group(rc, group)
            return spec.get("replicas", 0) if spec else 0
        except Exception:
            return -1

    def _query_float(self, query: str) -> Optional[float]:
        """执行 VM instant query，返回首个浮点值。"""
        try:
            resp = requests.get(
                f"{self.vm_url}/api/v1/query",
                params={"query": query},
                timeout=10,
            )
            resp.raise_for_status()
            logger.info(query)
            items = resp.json().get("data", {}).get("result", [])
            logger.info(items)
            if items:
                val = float(items[0]["value"][1])
                # print(val)
                return None if math.isnan(val) else val
            return None
        except Exception as exc:
            logger.debug("VM query failed: %s | query=%s", exc, query[:80])
            return None

# ─────────────────────────────────────────────
# 2. 稳态容量规划器
# ─────────────────────────────────────────────


@dataclass
class CapacityPlan:
    """容量规划结果。"""
    base_replicas: int                 # 利特尔法则基准副本数
    safe_replicas: int                 # 安全水位（base + buffer）
    concurrency_per_replica: float     # 单副本可承载并发任务数
    buffer_replicas: int               # 缓冲副本数
    reason: str                        # 决策理由


class CapacityPlanner:
    """基于利特尔法则（Little's Law）计算稳态所需副本数。

    公式:
        in_flight_tasks = peak_qps × p99_latency_sec
        concurrency_per_replica = min(cpu_per_replica / task_cpu,
                                       mem_per_replica / task_mem)
        base_replicas = ceil(in_flight_tasks / concurrency_per_replica)
        safe_replicas = base_replicas + buffer
    """

    def __init__(self, config: dict):
        self.cpu_per_replica = config.get("cpu_per_replica", 1.0)
        self.mem_per_replica = config.get("mem_mb_per_replica", 1024)
        self.buffer_ratio = config.get("buffer_ratio", 0.2)
        self.min_buffer = config.get("min_buffer", 1)

    def plan(self, peak_qps: float, p99_latency_ms: float,
             p99_cpu: float, p99_mem: float,
             min_replicas: int, max_replicas: int) -> CapacityPlan:
        """计算目标副本数。返回带缓冲的安全水位。"""
        task_cpu = max(p99_cpu, 0.01)
        task_mem = max(p99_mem, 1.0)

        cpu_concurrency = max(self.cpu_per_replica / task_cpu, 1.0)
        mem_concurrency = max(self.mem_per_replica / task_mem, 1.0)
        concurrency_per_replica = min(cpu_concurrency, mem_concurrency)

        latency_sec = max(p99_latency_ms, 0) / 1000.0
        in_flight_tasks = peak_qps * latency_sec

        base_replicas = max(math.ceil(in_flight_tasks / max(concurrency_per_replica, 1.0)), 1)
        buffer = max(math.ceil(base_replicas * self.buffer_ratio), self.min_buffer)
        safe_replicas = base_replicas + buffer

        base_replicas = max(min_replicas, min(base_replicas, max_replicas))
        safe_replicas = max(min_replicas, min(safe_replicas, max_replicas))

        reason = (
            f"Little's Law: λ={peak_qps:.1f}qps × W={p99_latency_ms:.0f}ms → "
            f"L={in_flight_tasks:.1f} tasks, "
            f"concurrency/rep={concurrency_per_replica:.1f}, "
            f"base={base_replicas}, buffer={buffer}, safe={safe_replicas}"
        )
        return CapacityPlan(base_replicas, safe_replicas,
                            concurrency_per_replica, buffer, reason)

# ─────────────────────────────────────────────
# 3. 计算驱动扩缩决策器
# ─────────────────────────────────────────────


@dataclass
class ScaleDecision:
    """单次扩缩容决策。"""
    action: str             # "scale_up" | "scale_down" | "none"
    target_replicas: int    # 目标副本数
    current_replicas: int   # 当前副本数
    reason: str             # 决策理由


class ComputeScaler:
    """纯计算驱动的扩缩容决策。

    规则:
        current < base  → 紧急扩容到 safe
        current > safe  → 保守缩容到 base
        其他情况不操作
    """

    def decide(self, current_replicas: int, plan: CapacityPlan,
               min_replicas: int, max_replicas: int) -> ScaleDecision:
        cur = current_replicas
        if cur < 0:
            return ScaleDecision("none", cur, cur, "unknown current replicas")

        if cur < plan.base_replicas:
            target = min(plan.safe_replicas, max_replicas)
            target = max(target, min_replicas)
            return ScaleDecision(
                "scale_up", target, cur,
                f"当前={cur} < 基准={plan.base_replicas}, "
                f"扩容至安全水位={target} "
                f"(缓冲={plan.buffer_replicas})")

        if cur > plan.safe_replicas:
            target = max(plan.base_replicas, min_replicas)
            return ScaleDecision(
                "scale_down", target, cur,
                f"当前={cur} > 安全={plan.safe_replicas}, "
                f"缩容至基准={target} "
                f"(缓冲={plan.buffer_replicas})")

        return ScaleDecision(
            "none", cur, cur,
            f"当前={cur} 在范围内 [{plan.base_replicas}, {plan.safe_replicas}]")

# ─────────────────────────────────────────────
# 4. 振荡抑制器
# ─────────────────────────────────────────────


@dataclass
class SuppressResult:
    """振荡抑制检查结果。"""
    allowed: bool
    reason: str


class OscillationSuppressor:
    """防振荡：冷却窗口 + 频率限制。

    扩容冷却短（快扩），缩容冷却长（慢缩）。
    """

    def __init__(self, config: dict):
        self.scale_up_cooldown = config.get("scale_up_cooldown", 30)
        self.scale_down_cooldown = config.get("scale_down_cooldown", 60)
        self.max_scale_up_per_cycle = config.get("max_scale_up_per_cycle", 3)
        self.max_scale_down_per_cycle = config.get("max_scale_down_per_cycle", 1)
        self._history: list = []

    def check(self, decision: ScaleDecision) -> SuppressResult:
        if decision.action == "none":
            return SuppressResult(True, "no action needed")

        now = time.time()
        cutoff = now - 300
        self._history = [(t, a) for t, a in self._history if t > cutoff]

        cooldown = (self.scale_up_cooldown if decision.action == "scale_up"
                    else self.scale_down_cooldown)

        for ts, action_type in reversed(self._history):
            remaining = cooldown - (now - ts)
            if remaining > 0:
                return SuppressResult(
                    False,
                    f"冷却剩余 {remaining:.0f}s (上次 {action_type})")

        recent_same = sum(1 for t, a in self._history if a == decision.action)
        max_allowed = (self.max_scale_up_per_cycle if decision.action == "scale_up"
                       else self.max_scale_down_per_cycle)
        if recent_same >= max_allowed:
            return SuppressResult(
                False,
                f"周期内已达上限 {max_allowed} 次 ({decision.action})")

        return SuppressResult(True, "冷却已过")

    def record(self, decision: ScaleDecision):
        if decision.action != "none":
            self._history.append((time.time(), decision.action))
            if len(self._history) > 100:
                self._history = self._history[-100:]

# ─────────────────────────────────────────────
# 5. 主控制循环
# ─────────────────────────────────────────────


class SchedulerController:
    """自动弹性伸缩调度器主循环。

    每 scrape_interval 秒执行一轮：
        1. 采集 VM 指标
        2. 利特尔法则计算目标容量
        3. 比较当前副本数，决定扩缩
        4. 振荡抑制器检查冷却
        5. 执行 K8s 扩缩容
    """

    def __init__(self, config_path: str):
        with open(config_path) as f:
            cfg = yaml.safe_load(f)

        self.vm_url = cfg["prometheus_url"]
        self.interval = cfg.get("scrape_interval", 10)
        self.stages = cfg.get("stages", [])
        capacity_cfg = cfg.get("capacity", {})
        suppress_cfg = cfg.get("suppressor", {})

        self.collector = MetricsCollector(self.vm_url)
        self.planner = CapacityPlanner(capacity_cfg)
        self.scaler = ComputeScaler()
        self.suppressor = OscillationSuppressor(suppress_cfg)
        self.k8s_scaler = RayClusterScaler(namespace=cfg.get("namespace", "default"))

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _run_once(self) -> dict:
        cycle_start = time.time()
        decisions: dict = {
            "cycle_start": datetime.now(timezone.utc).isoformat(),
            "stages": {},
        }

        for sc in self.stages:
            sid = sc["stage_id"]
            cluster = sc.get("cluster", "raycluster-kuberay")
            group = sc.get("worker_group", "workergroup")
            window = sc.get("window", "1m")
            qps_window = sc.get("peak_qps_window", window)
            min_rep = sc.get("min_replicas", 1)
            max_rep = sc.get("max_replicas", 20)

            peak_qps = self.collector.query_peak_qps(sid, qps_window)
            logger.info(f'peak_qps: {peak_qps}')
            p99_lat = self.collector.query_p99_latency(sid, window)
            p99_queue = self.collector.query_p99_queue(sid, window)
            p99_cpu = self.collector.query_p99_cpu(sid, window)
            p99_mem = self.collector.query_p99_mem(sid, window)
            cur_rep = self.collector.query_current_replicas(self.k8s_scaler, cluster, group)

            stage_data = {
                "peak_qps": peak_qps,
                "p99_latency_ms": p99_lat,
                "p99_queue_ms": p99_queue,
                "p99_cpu_cores": p99_cpu,
                "p99_memory_mb": p99_mem,
                "current_replicas": cur_rep,
            }
            # print(peak_qps)

            if peak_qps is None:
                peak_qps = 0.0

            if peak_qps > 0.0:
                has_essential = all(v is not None for v in [p99_lat, p99_cpu, p99_mem])
                if not has_essential:
                    stage_data["decision"] = "skip_no_data"
                    decisions["stages"][sid] = stage_data
                    logger.info("Skip stage=%s: insufficient metrics (need lat, cpu, mem)", sid)
                    continue

                plan = self.planner.plan(
                    peak_qps, p99_lat, p99_cpu, p99_mem, min_rep, max_rep)
            else:
                logger.info("Stage=%s: peak_qps=0, scaling down regardless of other metrics", sid)
                plan = CapacityPlan(
                    base_replicas=0,
                    safe_replicas=min_rep,
                    concurrency_per_replica=1.0,
                    buffer_replicas=0,
                    reason=f"No traffic: peak_qps=0, scale to min={min_rep}",
                )

            stage_data["capacity_plan"] = {
                "base_replicas": plan.base_replicas,
                "safe_replicas": plan.safe_replicas,
                "buffer_replicas": plan.buffer_replicas,
                "concurrency_per_replica": plan.concurrency_per_replica,
                "reason": plan.reason,
            }

            decision = self.scaler.decide(cur_rep, plan, min_rep, max_rep)
            stage_data["scale_decision"] = {
                "action": decision.action,
                "target_replicas": decision.target_replicas,
                "reason": decision.reason,
            }

            suppress = self.suppressor.check(decision)
            stage_data["suppress"] = {
                "allowed": suppress.allowed,
                "reason": suppress.reason,
            }

            if not suppress.allowed:
                decisions["stages"][sid] = stage_data
                logger.info(
                    "Stage=%s suppressed: %s", sid, suppress.reason)
                continue

            if decision.action == "scale_up":
                result = self.k8s_scaler.scale_set(cluster, group, decision.target_replicas)
            elif decision.action == "scale_down":
                result = self.k8s_scaler.scale_set(cluster, group, decision.target_replicas)
            else:
                result = {"success": True, "message": "no action"}

            stage_data["execute_result"] = result

            if result.get("success") and decision.action != "none":
                self.suppressor.record(decision)

            decisions["stages"][sid] = stage_data

        decisions["cycle_ms"] = round((time.time() - cycle_start) * 1000, 1)
        logger.info(
            "Cycle: %s",
            ", ".join(
                f"{s}: {d.get('scale_decision', {}).get('action', '?')}→"
                f"{d.get('scale_decision', {}).get('target_replicas', '?')}"
                for s, d in decisions.get("stages", {}).items()
            ),
        )
        return decisions

    def _loop(self):
        logger.info("SchedulerController daemon loop started")
        while not self._stop.wait(self.interval):
            try:
                self._run_once()
            except Exception as exc:
                logger.error("Scheduler cycle failed: %s", exc, exc_info=True)

    def start(self):
        if self._thread is None:
            self._thread = threading.Thread(target=self._loop, daemon=True)
            self._thread.start()
            logger.info("SchedulerController started (interval=%ss)", self.interval)

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
            logger.info("SchedulerController stopped")
