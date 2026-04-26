import threading
import logging
import yaml
from datetime import datetime
from typing import Optional
from prometheus_api_client import PrometheusConnect

logger = logging.getLogger(__name__)

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

        self.prom = PrometheusConnect(url=cfg["prometheus_url"], disable_ssl=True)
        self.interval = cfg["scrape_interval"]
        self.stages = cfg["stages"]
        self._stop = threading.Event()
        self.latest: dict = {}

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
            f'histogram_quantile({p}, '
            f'sum by (le) (rate(ray_proxy_queue_time_ms_bucket{{stage_id="{sid}"}}[{w}]))'
        )
        lat_q = (
            f'histogram_quantile({p}, '
            f'sum by (le) (rate(ray_proxy_stage_latency_ms_bucket{{stage_id="{sid}"}}[{w}]))'
        )

        for key, query in [("queue_time_ms", queue_q), ("latency_ms", lat_q)]:
            try:
                resp = self.prom.custom_query(query)
                result[key] = float(resp[0]["value"][1]) if resp else None
            except Exception as e:
                result[key] = None
                result[f"{key}_error"] = str(e)

        return result

    def _run_once(self):
        now = datetime.utcnow().isoformat()
        for sc in self.stages:
            self.latest[sc["stage_id"]] = self._query_pxx(sc)
        self.latest["_meta"] = {"last_updated": now}
        logger.info(f"LatencyController updated at {now}")

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
