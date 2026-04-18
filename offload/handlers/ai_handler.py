import ray
from typing import Any, List
from .base import BaseTaskHandler
from ..task_plan import TaskPlan

class AIInferenceHandler(BaseTaskHandler):
    def preprocess(self, raw_data: bytes) -> Any:
        return raw_data

    def build_plan(self, data: Any) -> TaskPlan:
        is_large = isinstance(data, (bytes, bytearray)) and len(data) > 1024 * 1024

        if is_large:
            data_ref = ray.put(data)
            chunks = [data_ref for _ in range(4)]
            resources = [{"CPU": 1.0, "GPU": 0.5} for _ in range(4)]
        else:
            chunks = [data] * 4
            resources = [{"CPU": 0.5, "GPU": 0.2} for _ in range(4)]

        def ai_worker(chunk):
            import time
            import ray
            time.sleep(0.5)
            node = ray.get_runtime_context().get_node_id()[:6]
            if isinstance(chunk, ray.ObjectRef):
                chunk = ray.get(chunk)
            size = len(chunk) if isinstance(chunk, (bytes, bytearray)) else 0
            return f"AI infer done on {node}, data size={size}"

        def aggregator(results: List[Any]) -> str:
            return f"AI 推理完成，共 {len(results)} 路分片通过共享内存并行处理成功"

        return TaskPlan(
            chunks=chunks,
            chunk_resources=resources,
            worker_fn=ai_worker,
            aggregator_fn=aggregator,
            display_formatter=str
        )