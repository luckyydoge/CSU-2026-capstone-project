import time
from datetime import datetime
from enum import Enum
from typing import Callable, Any, Optional, List
import uuid
import psutil
import os
import threading
from ray.util.metrics import Gauge, Histogram

_cpu_usage = Gauge('proxy_actor_cpu_usage', tag_keys=('node_id', 'stage_id'))
_mem_usage = Gauge('proxy_actor_mem_usage', tag_keys=('node_id', 'stage_id'))
_actors = Gauge('proxy_actors', tag_keys=('node_id', 'stage_id'))
_stage_latency = Histogram(
    'proxy_stage_latency_ms_test',
    tag_keys=('node_id', 'stage_id'),
    boundaries=[10, 50, 100, 200, 500, 1000, 2000, 5000, 10000],
)


class SubmissionType(Enum):
    UNKNOWN = 0
    TASK = 1
    ACTOR = 2


def monitor(stage_id: str = "", **ray_kwargs: Any):
    def decorator(target: Callable):
        if isinstance(target, type):
            origin_init = target.__init__
            def _monitor_loop(self):
                while not self._stop_event.is_set():
                    _cpu_usage.set(self.process.cpu_percent(), tags={'node_id': self._node_id, 'stage_id': stage_id})
                    _mem_usage.set(self.process.memory_info().rss / 1024 / 1024, tags={'node_id': self._node_id, 'stage_id': stage_id})
                    time.sleep(5)
            target._monitor_loop = _monitor_loop

            def new_init(self, *args, **kwargs):
                origin_init(self, *args, **kwargs)
                self._stop_event = None
                self.proxy_actor_id = str(uuid.uuid4())[:8]

                import ray
                try:
                    self._node_id = ray.get_runtime_context().get_node_id()
                except Exception:
                    self._node_id = "unknown"

                self.process = psutil.Process(os.getpid())

                self._stop_event = threading.Event()
                self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
                self._monitor_thread.start()
            target.__init__ = new_init

            origin_del = getattr(target, '__del__', None)

            def new_del(self):
                if origin_del:
                    origin_del(self)
                if hasattr(self, '_stop_event') and self._stop_event is not None:
                    self._stop_event.set()
            target.__del__ = new_del

            return target

        def wrapper(*args, **kwargs):
            import ray
            try:
                node_id = ray.get_runtime_context().get_node_id()
            except Exception:
                node_id = "unknown"

            process = psutil.Process(os.getpid())

            _stop_event = threading.Event()
            def _monitor_loop():
                tags = {'node_id': node_id, 'stage_id': stage_id}
                while not _stop_event.is_set():
                    _cpu_usage.set(process.cpu_percent(), tags=tags)
                    _mem_usage.set(process.memory_info().rss / 1024 / 1024, tags=tags)
                    _actors.set(1, tags=tags)
                    time.sleep(5)

            _monitor_thread = threading.Thread(target=_monitor_loop, daemon=True)
            _monitor_thread.start()

            actual_start_time = datetime.utcnow()
            retval = target(*args, **kwargs)
            actual_end_time = datetime.utcnow()
            _stop_event.set()

            latency_ms = (actual_end_time - actual_start_time).total_seconds() * 1000
            _stage_latency.observe(latency_ms, tags={'node_id': node_id, 'stage_id': stage_id})

            if isinstance(retval, dict):
                retval["_actual_start_time"] = actual_start_time.isoformat()
                retval["_actual_end_time"] = actual_end_time.isoformat()
                retval["_latency_ms"] = latency_ms

            return retval

        return wrapper

    return decorator
