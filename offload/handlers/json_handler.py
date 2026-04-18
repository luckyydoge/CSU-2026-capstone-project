import json
from typing import Any, List
from .base import BaseTaskHandler
from ..task_plan import TaskPlan

class JsonDataHandler(BaseTaskHandler):
    def preprocess(self, raw_data: bytes) -> Any:
        return json.loads(raw_data.decode("utf-8"))

    def build_plan(self, data: Any) -> TaskPlan:
        if not isinstance(data, list):
            data = [data]

        line_count = len(data)
        mid = line_count // 2

        chunks = [data[:mid], data[mid:]]

        resources = []
        for chunk in chunks:
            cpu_need = round(max(0.1, len(chunk) / 1000.0), 2)
            resources.append({"CPU": cpu_need})

        def json_worker(chunk):
            import time
            time.sleep(0.2)
            return f"Processed {len(chunk)} items"

        def aggregator(results: List[Any]) -> List[str]:
            return results

        def formatter(result: Any) -> List[str]:
            return result[:10] if isinstance(result, list) else [str(result)]

        return TaskPlan(
            chunks=chunks,
            chunk_resources=resources,
            worker_fn=json_worker,
            aggregator_fn=aggregator,
            display_formatter=formatter
        )