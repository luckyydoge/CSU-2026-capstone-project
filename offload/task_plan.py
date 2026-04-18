from dataclasses import dataclass, field
from typing import List, Any, Dict, Callable, Optional

@dataclass
class TaskPlan:
    """
    任务执行计划：包含分片、每个分片的资源需求、worker函数、聚合函数等。
    """
    chunks: List[Any]                               # 待处理的分片数据
    chunk_resources: List[Dict[str, float]]         # 每个分片的资源需求，与 chunks 一一对应
    worker_fn: Optional[Callable[[Any], Any]] = None # 分片处理函数，若为 None 则使用默认 worker
    aggregator_fn: Optional[Callable[[List[Any]], Any]] = None  # 聚合函数
    display_formatter: Optional[Callable[[Any], Any]] = None    # 最终格式化函数

    def __post_init__(self):
        # 简单校验：chunks 和 chunk_resources 长度必须一致
        if len(self.chunks) != len(self.chunk_resources):
            raise ValueError("chunks 与 chunk_resources 长度必须一致")