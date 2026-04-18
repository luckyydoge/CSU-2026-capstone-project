import json
from typing import Any, List
from .base import BaseTaskHandler
from ..task_plan import TaskPlan

class HybridInferenceHandler(BaseTaskHandler):
    def preprocess(self, raw_data: bytes) -> Any:
        return json.loads(raw_data.decode("utf-8"))

    def build_plan(self, data: dict) -> TaskPlan:
        images = data.get("images", [])
        texts = data.get("texts", [])

        chunks = []
        resources = []

        for img in images:
            chunks.append(img)
            size_mb = len(img) / 1024 / 1024 if isinstance(img, bytes) else 1.0
            resources.append({
                "CPU": 0.5,
                "GPU": 0.5 if size_mb > 5 else 0.2
            })

        batch_size = 10
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i+batch_size]
            chunks.append(batch)
            resources.append({"CPU": 0.2})

        def hybrid_worker(chunk):
            if isinstance(chunk, str) and chunk.startswith("/9j/"):
                return {"type": "image", "label": "cat", "confidence": 0.95}
            elif isinstance(chunk, list):
                return {"type": "text", "count": len(chunk)}
            else:
                return {"type": "unknown"}

        def aggregator(results: List[Any]) -> dict:
            return {
                "image_results": [r for r in results if isinstance(r, dict) and r.get("type") == "image"],
                "text_results": [r for r in results if isinstance(r, dict) and r.get("type") == "text"],
                "other": [r for r in results if not isinstance(r, dict)]
            }

        return TaskPlan(
            chunks=chunks,
            chunk_resources=resources,
            worker_fn=hybrid_worker,
            aggregator_fn=aggregator,
            display_formatter=lambda x: x   # 原样返回
        )