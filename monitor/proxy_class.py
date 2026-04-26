import time
from datetime import datetime
from enum import Enum
from typing import Callable, Any, Optional, List
import uuid
import psutil
import os
import threading
from ray.util.metrics import Gauge

class SubmissionType(Enum):
    UNKNOWN = 0
    TASK = 1
    ACTOR = 2


def monitor(submission_id: str, proxy_id: Optional[str] = None, **ray_kwargs: Any):
    def decorator(target: Callable):        
        if isinstance(target, type):
            print("decorator")
            origin_init = target.__init__
            def _monitor_loop(self):
                while not self._stop_event.is_set():
                    # 直接更新共享的 gauge 指标
                    self.cpu_usage.set(self.process.cpu_percent())
                    self.mem_usage.set(self.process.memory_info().rss / 1024 / 1024)
                    self.test.set(1)
                    time.sleep(5)
            target._monitor_loop = _monitor_loop

            def new_init(self, *args, **kwargs):
                origin_init(self, *args, **kwargs)
                self._stop_event = None
                self.proxy_actor_id = str(uuid.uuid4())[:8]
                self.submission_id = submission_id
                
                self.process = psutil.Process(os.getpid())
                self.func = None

                tags = {'actor_id': self.proxy_actor_id, 'submission_id': submission_id}

                self.test = Gauge(
                    'proxy_actors',
                    description = 'test',
                    tag_keys=('submission_id', 'actor_id')
                )
                self.cpu_usage = Gauge(
                    'proxy_actor_cpu_usage',
                    description = 'test',
                    tag_keys=('submission_id', 'actor_id')
                )
                self.mem_usage = Gauge(
                    'proxy_actor_mem_usage',
                    description = 'test',
                    tag_keys=('submission_id', 'actor_id')
                )

                self.test.set_default_tags(tags)
                self.mem_usage.set_default_tags(tags)
                self.cpu_usage.set_default_tags(tags)

                self.target_type = SubmissionType.ACTOR
                    
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
            _stop_event = None
            proxy_actor_id = str(uuid.uuid4())[:8]

            process = psutil.Process(os.getpid())
            func = None

            tags = {'actor_id': proxy_actor_id, 'submission_id': submission_id}

            test = Gauge(
                'proxy_actors',
                description = 'test',
                tag_keys=('submission_id', 'actor_id')
            )
            cpu_usage = Gauge(
                'proxy_actor_cpu_usage',
                description = 'test',
                tag_keys=('submission_id', 'actor_id')
            )
            mem_usage = Gauge(
                'proxy_actor_mem_usage',
                description = 'test',
                tag_keys=('submission_id', 'actor_id')
            )

            test.set_default_tags(tags)
            mem_usage.set_default_tags(tags)
            cpu_usage.set_default_tags(tags)

            target_type = SubmissionType.ACTOR

            _stop_event = threading.Event()
            def _monitor_loop():
                while not _stop_event.is_set():
                    # 直接更新共享的 gauge 指标
                    cpu_usage.set(process.cpu_percent())
                    mem_usage.set(process.memory_info().rss / 1024 / 1024)
                    test.set(1)
                    time.sleep(5)

            _monitor_thread = threading.Thread(target=_monitor_loop, daemon=True)
            _monitor_thread.start()

            actual_start_time = datetime.utcnow()
            retval = target(*args, **kwargs)
            _stop_event.set()

            if isinstance(retval, dict):
                retval["_actual_start_time"] = actual_start_time.isoformat()

            return retval
        
        return wrapper
            
    return decorator
