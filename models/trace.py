from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class StepRecord(BaseModel):
    step_index: int
    stage_name: str
    node_id: str
    node_tier: str
    start_time: datetime
    end_time: datetime
    execution_time_ms: float
    transfer_time_ms: float = 0.0
    input_size_bytes: Optional[int] = None
    output_size_bytes: Optional[int] = None
    cpu_percent: Optional[float] = None
    memory_mb: Optional[int] = None
    error_msg: Optional[str] = None

class ExecutionTrace(BaseModel):
    task_id: str
    execution_path: List[StepRecord] = []
    total_latency_ms: float = 0.0
    total_transfer_overhead_ms: float = 0.0
    error_logs: List[str] = []