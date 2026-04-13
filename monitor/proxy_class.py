import time
from enum import Enum
from typing import Callable, Any, Optional, List
import uuid
import psutil
import os
import threading
from ray.util.metrics import Gauge
import ray

class SubmissionType(Enum):
    UNKNOWN = 0
    TASK = 1
    ACTOR = 2

@ray.remote
class ProxyActor:
    def __init__(self,
                 proxy_actor_id: Optional[str],
                 submission_id: str = "Unknown",
                 submission_type: SubmissionType = SubmissionType.UNKNOWN,
                 func: Optional[Callable[..., Any]] = None,
                 actor: Optional[Any] = None) -> None:
        self.proxy_actor_id = proxy_actor_id if proxy_actor_id else str(uuid.uuid4())[:8]
        self.submission_id = submission_id
        self.submission_type = submission_type
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
        
        
        
        match submission_type:
            case SubmissionType.TASK:
                self.func = func
            case SubmissionType.ACTOR:
                pass
            case _:
                raise NotImplementedError(f'类型 {submission_type} 处理未实现')

        self._stop_event = threading.Event()
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()

            
    def _monitor_loop(self):
        while not self._stop_event.is_set():
            # 直接更新共享的 gauge 指标
            self.cpu_usage.set(self.process.cpu_percent())
            self.mem_usage.set(self.process.memory_info().rss / 1024 / 1024)
            self.test.set(1)
            time.sleep(5)

    def __del__(self):
        self._stop_event.set()
        
    def execute(self, *args, **kwargs):
        if self.submission_type != SubmissionType.TASK:
            raise TypeError(f'该提交不是task类型')
        if self.func:
            return self.func(*args, **kwargs)
        else :
            raise AttributeError(f'未赋值func')
        


class ProxyFactory:
    def __init__(self) -> None:
        pass
    submissions = {}
    
    @classmethod
    def create(cls, tasks: List[Callable[..., Any]], submission_id: str) -> List['ray.actor.ActorHandler']:
        actors = []
        actor_ids = []

        for task in tasks:
            proxy_actor_id = str(uuid.uuid4())[:8]
            proxy_actor = ProxyActor.remote(proxy_actor_id, submission_id, SubmissionType.TASK, task)

            actors.append(proxy_actor)
            actor_ids.append(proxy_actor_id)
        cls.submissions[submission_id] = actor_ids
        
        return actors

    @classmethod
    def get_submissions(cls) -> List[str]:
        return list(cls.submissions.keys())

    @classmethod
    def get_proxy_actors_id_by_submission_id(cls, submission_id: str) -> List[str]:
        return cls.submissions[submission_id]
        
        
