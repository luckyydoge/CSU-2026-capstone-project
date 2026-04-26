import math
import re
import time
import threading
import logging
import yaml
import requests
from datetime import datetime, timezone
from typing import Optional
from app.k8s_scaler import RayClusterScaler

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

_controller: Optional["LatencyController"] = None


def get_controller() -> Optional["LatencyController"]:
    return _controller


def init_controller(config_path: str) -> "LatencyController":
    global _controller
    _controller = LatencyController(config_path)
    _controller.start()
    return _controller


class LatencyController:
    def __init__(self, config_path: str):
        with open(config_path) as f:
            cfg = yaml.safe_load(f)

        self.prom_url = cfg["prometheus_url"]
        self.interval = cfg["scrape_interval"]
        self.stages = cfg["stages"]
        self._stop = threading.Event()
        self.latest: dict = {}
        self.scaler = RayClusterScaler(namespace="default")
        self._last_action: dict = {}

    def _query_vm(self, query: str) -> list:
        try:
            r = requests.get(f"{self.prom_url}/api/v1/query", params={"query": query}, timeout=10)
            r.raise_for_status()
            return r.json()["data"]["result"]
        except Exception:
            return []

    @staticmethod
    def _parse_window(w: str) -> int:
        m = re.match(r"(\d+)([smhd])", w)
        if not m:
            return 60
        val, unit = int(m.group(1)), m.group(2)
        return {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit, 1) * val

    def _query_pxx(self, stage_cfg: dict) -> dict:
        sid = stage_cfg["stage_id"]
        p = stage_cfg["percentile"]
        w = stage_cfg["window"]
        result = {
            "stage_id": sid,
            "percentile": p,
            "window": w,
            "worker_label": stage_cfg.get("worker_label", ""),
        }

        queue_q = (
            f'sum(quantile_over_time({p}, '
            f'proxy_queue_time_ms{{stage_id="{sid}"}}[{w}])) by (stage_id)'
        )
        lat_q = (
            f'sum(quantile_over_time({p}, '
            f'proxy_stage_latency_ms{{stage_id="{sid}"}}[{w}])) by (stage_id)'
        )

        for key, query in [("queue_time_ms", queue_q), ("latency_ms", lat_q)]:
            try:
                items = self._query_vm(query)
                if items:
                    val = float(items[0]["value"][1])
                    result[key] = val if not math.isnan(val) else None
                else:
                    result[key] = None
            except Exception as e:
                result[key] = None
                result[f"{key}_error"] = str(e)

        return result

    def _query_resources_pxx(self, stage_cfg: dict) -> dict:
        sid = stage_cfg["stage_id"]
        p = stage_cfg["percentile"]
        w = stage_cfg["window"]
        result = {"stage_id": sid}

        cpu_q = f'quantile_over_time({p}, proxy_task_cpu_cores{{stage_id="{sid}"}}[{w}])'
        mem_q = f'quantile_over_time({p}, proxy_task_memory_mb{{stage_id="{sid}"}}[{w}])'

        for key, q in [("cpu_cores_p99", cpu_q), ("memory_mb_p99", mem_q)]:
            try:
                items = self._query_vm(q)
                if items:
                    val = float(items[0]["value"][1])
                    result[key] = val if not math.isnan(val) else None
                else:
                    result[key] = None
            except Exception as e:
                result[key] = None
                result[f"{key}_error"] = str(e)

        return result

    def _query_qps(self, stage_cfg: dict) -> dict:
        sid = stage_cfg["stage_id"]
        w = stage_cfg["window"]
        window_sec = self._parse_window(w)
        result = {"stage_id": sid}

        q = f'count(proxy_stage_latency_ms{{stage_id="{sid}"}}[{w}])'
        try:
            items = self._query_vm(q)
            total = float(items[0]["value"][1]) if items else 0
            result["qps"] = round(total / window_sec, 2) if window_sec > 0 else 0
            result["total_count"] = int(total)
        except Exception as e:
            result["qps"] = None
            result["qps_error"] = str(e)

        return result

    def _run_once(self):
        now = datetime.now(timezone.utc).isoformat()
        for sc in self.stages:
            sid = sc["stage_id"]
            self.latest[sid] = self._query_pxx(sc)
            self.latest[sid].update(self._query_resources_pxx(sc))
            self.latest[sid].update(self._query_qps(sc))
        self.latest["_meta"] = {"last_updated": now}
        self._evaluate_rules()
        logger.info(f"LatencyController updated at {now}")

    def _evaluate_rules(self):
        for sc in self.stages:
            sid = sc["stage_id"]
            for i, rule in enumerate(sc.get("rules", [])):
                metric_val = self.latest.get(sid, {}).get(rule["metric"])
                if metric_val is None:
                    logger.info(f"Rule skip: stage={sid} {rule['metric']}=None no data")
                    continue

                key = (sid, i)
                last = self._last_action.get(key, 0.0)
                remaining = rule.get("cooldown", 60) - (time.time() - last)
                if remaining > 0:
                    logger.info(
                        f"Rule skip: cooldown active stage={sid} "
                        f"{rule['metric']}={metric_val:.1f} remaining={remaining:.0f}s"
                    )
                    continue

                logger.info(
                    f"Rule eval: stage={sid} {rule['metric']}={metric_val:.1f} "
                    f"op={rule['operator']} threshold={rule['threshold']}"
                )

                triggered = False
                if rule["operator"] == ">" and metric_val > rule["threshold"]:
                    triggered = True
                elif rule["operator"] == "<" and metric_val < rule["threshold"]:
                    triggered = True

                if not triggered:
                    logger.info(
                        f"Rule skip: threshold not hit stage={sid} "
                        f"{metric_val:.1f} {rule['operator']} {rule['threshold']}"
                    )
                    continue

                logger.info(f"Rule TRIGGERED: stage={sid} {rule['metric']}={metric_val:.1f}")
                for action in rule.get("actions", []):
                    fn = getattr(self.scaler, action["type"], None)
                    if fn:
                        kwargs = {k: v for k, v in action.items() if k != "type"}
                        result = fn(**kwargs)
                        logger.info(
                            f"Action executed: stage={sid} {rule['metric']}={metric_val:.1f} "
                            f"type={action['type']} result={result}"
                        )
                self._last_action[key] = time.time()

    def _loop(self):
        self._run_once()
        while not self._stop.wait(self.interval):
            self._run_once()

    def start(self):
        if not hasattr(self, "_thread") or self._thread is None:
            self._thread = threading.Thread(target=self._loop, daemon=True)
            self._thread.start()
            logger.info(f"LatencyController started (interval={self.interval}s)")

    def stop(self):
        self._stop.set()
        if hasattr(self, "_thread") and self._thread:
            self._thread.join(timeout=5)
            logger.info("LatencyController stopped")
