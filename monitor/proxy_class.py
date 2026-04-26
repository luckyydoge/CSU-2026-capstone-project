import time
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Any, Optional, List
import uuid
import psutil
import os
import threading
from ray.util.metrics import Gauge, Histogram
from monitor.vm_writer import push_metric, get_worker_group

_cpu_usage = Gauge('proxy_actor_cpu_usage', tag_keys=('node_id', 'stage_id', 'class_name'))
_mem_usage = Gauge('proxy_actor_mem_usage', tag_keys=('node_id', 'stage_id', 'class_name'))
_actors = Gauge('proxy_actors', tag_keys=('node_id', 'stage_id', 'class_name'))
_stage_latency = Histogram(
    'proxy_stage_latency_ms',
    tag_keys=('node_id', 'stage_id', 'class_name'),
    boundaries=[10, 50, 100, 200, 500, 1000, 2000, 5000, 10000],
)
_queue_latency = Histogram(
    'proxy_queue_time_ms',
    tag_keys=('node_id', 'stage_id', 'class_name'),
    boundaries=[10, 50, 100, 200, 500, 1000, 2000, 5000, 10000],
)


class SubmissionType(Enum):
    UNKNOWN = 0
    TASK = 1
    ACTOR = 2


def monitor(stage_id: str = "", submit_time: str = "", **ray_kwargs: Any):
    def decorator(target: Callable):
        if isinstance(target, type):
            cls_name = target.__name__
            origin_init = target.__init__
            def _monitor_loop(self):
                wg = get_worker_group()
                tags = {'node_id': self._node_id, 'stage_id': stage_id, 'class_name': cls_name}
                while not self._stop_event.is_set():
                    cpu_val = self.process.cpu_percent()
                    mem_val = self.process.memory_info().rss / 1024 / 1024
                    _cpu_usage.set(cpu_val, tags=tags)
                    _mem_usage.set(mem_val, tags=tags)
                    push_metric("proxy_actor_cpu_usage", cpu_val, dict(tags, worker_group=wg))
                    push_metric("proxy_actor_mem_usage", mem_val, dict(tags, worker_group=wg))
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

            # 包装 __call__，记录排队时延和执行时延
            origin_call = target.__call__

            def new_call(self, *args, **kwargs):
                import ray
                try:
                    node_id = ray.get_runtime_context().get_node_id()
                except Exception:
                    node_id = "unknown"

                wg = get_worker_group()
                tags_vm = {'node_id': node_id, 'stage_id': stage_id, 'worker_group': wg, 'class_name': cls_name}
                observe_tags = {'node_id': node_id, 'stage_id': stage_id, 'class_name': cls_name}

                cpu_cores = ray_kwargs.get("num_cpus", 0)
                mem_mb = ray_kwargs.get("memory_mb", 0)
                push_metric("proxy_task_cpu_cores", cpu_cores, tags_vm)
                push_metric("proxy_task_memory_mb", mem_mb, tags_vm)

                submit_time_str = ""
                for arg in args:
                    if isinstance(arg, dict) and "submit_time" in arg:
                        submit_time_str = arg.get("submit_time", "")
                        break

                start_ts = time.time()
                start_dt = datetime.utcnow()

                if submit_time_str:
                    submit_dt = datetime.fromisoformat(submit_time_str)
                    submit_ts = submit_dt.replace(tzinfo=timezone.utc).timestamp()
                    queue_ms = (start_ts - submit_ts) * 1000
                    _queue_latency.observe(queue_ms, tags=observe_tags)
                    if queue_ms >= 0:
                        push_metric("proxy_queue_time_ms", queue_ms, tags_vm)

                retval = origin_call(self, *args, **kwargs)
                end_ts = time.time()
                end_dt = datetime.utcnow()

                latency_ms = (end_ts - start_ts) * 1000
                _stage_latency.observe(latency_ms, tags=observe_tags)
                push_metric("proxy_stage_latency_ms", latency_ms, tags_vm)

                if isinstance(retval, dict):
                    retval["_actual_start_time"] = start_dt.isoformat()
                    retval["_actual_end_time"] = end_dt.isoformat()
                    retval["_latency_ms"] = latency_ms

                return retval

            target.__call__ = new_call

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
                tags = {'node_id': node_id, 'stage_id': stage_id, 'class_name': ''}
                wg = get_worker_group()
                while not _stop_event.is_set():
                    cpu_val = process.cpu_percent()
                    mem_val = process.memory_info().rss / 1024 / 1024
                    _cpu_usage.set(cpu_val, tags=tags)
                    _mem_usage.set(mem_val, tags=tags)
                    _actors.set(1, tags=tags)
                    push_metric("proxy_actor_cpu_usage", cpu_val, {'node_id': node_id, 'stage_id': stage_id, 'worker_group': wg, 'class_name': ''})
                    push_metric("proxy_actor_mem_usage", mem_val, {'node_id': node_id, 'stage_id': stage_id, 'worker_group': wg, 'class_name': ''})
                    push_metric("proxy_actors", 1, {'node_id': node_id, 'stage_id': stage_id, 'worker_group': wg, 'class_name': ''})
                    time.sleep(5)

            _monitor_thread = threading.Thread(target=_monitor_loop, daemon=True)
            _monitor_thread.start()

            tags_vm = {'node_id': node_id, 'stage_id': stage_id, 'worker_group': get_worker_group(), 'class_name': ''}
            observe_tags = {'node_id': node_id, 'stage_id': stage_id, 'class_name': ''}

            cpu_cores = ray_kwargs.get("num_cpus", 0)
            mem_mb = ray_kwargs.get("memory_mb", 0)
            push_metric("proxy_task_cpu_cores", cpu_cores, tags_vm)
            push_metric("proxy_task_memory_mb", mem_mb, tags_vm)

            start_ts = time.time()
            start_dt = datetime.utcnow()

            if submit_time:
                submit_dt = datetime.fromisoformat(submit_time)
                submit_ts = submit_dt.replace(tzinfo=timezone.utc).timestamp()
                queue_ms = (start_ts - submit_ts) * 1000
                _queue_latency.observe(queue_ms, tags=observe_tags)
                if queue_ms >= 0:
                    push_metric("proxy_queue_time_ms", queue_ms, tags_vm)

            retval = target(*args, **kwargs)
            end_ts = time.time()
            end_dt = datetime.utcnow()
            _stop_event.set()

            latency_ms = (end_ts - start_ts) * 1000
            _stage_latency.observe(latency_ms, tags=observe_tags)
            push_metric("proxy_stage_latency_ms", latency_ms, tags_vm)

            if isinstance(retval, dict):
                retval["_actual_start_time"] = start_dt.isoformat()
                retval["_actual_end_time"] = end_dt.isoformat()
                retval["_latency_ms"] = latency_ms

            return retval

        return wrapper

    return decorator
