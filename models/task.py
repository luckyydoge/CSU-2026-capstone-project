from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
from datetime import datetime
from enum import Enum

class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class TaskCreateRequest(BaseModel):
    application_name: str = Field(..., description="应用名称")
    strategy_name: str = Field(..., description="策略名称")
    input_data: Any = Field(..., description="输入数据")
    runtime_config: Optional[Dict[str, Any]] = Field(default_factory=dict)

class TaskResponse(BaseModel):
    task_id: str
    status: TaskStatus
    created_at: datetime

class TaskDetailResponse(BaseModel):
    task_id: str
    application_name: str
    strategy_name: str
    status: TaskStatus
    created_at: datetime
    completed_at: Optional[datetime] = None
    final_output: Optional[Any] = None
    # 注意：没有 trace 字段