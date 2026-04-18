from abc import ABC, abstractmethod
from typing import Any
from ..task_plan import TaskPlan

class BaseTaskHandler(ABC):
    @abstractmethod
    def preprocess(self, raw_data: bytes) -> Any:
        """将原始字节流转换为业务对象"""
        pass

    @abstractmethod
    def build_plan(self, data: Any) -> TaskPlan:
        """
        根据预处理后的数据生成任务执行计划。
        返回的 TaskPlan 包含分片、资源标签、worker、聚合器等。
        """
        pass