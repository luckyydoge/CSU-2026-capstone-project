from .handlers.json_handler import JsonDataHandler
from .handlers.ai_handler import AIInferenceHandler
from .handlers.hybrid_handler import HybridInferenceHandler
from .handlers.base import BaseTaskHandler

class HandlerFactory:
    _strategies = {
        "json": JsonDataHandler,
        "ai": AIInferenceHandler,
        "hybrid": HybridInferenceHandler,   # 新增混合任务类型
    }

    @classmethod
    def create_handler(cls, task_type: str) -> BaseTaskHandler:
        handler_class = cls._strategies.get(task_type.lower())
        if not handler_class:
            raise ValueError(f"未知任务类型: {task_type}")
        return handler_class()