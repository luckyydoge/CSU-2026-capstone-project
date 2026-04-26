from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class MonitorRecordCreate(BaseModel):
    name: str
    status: str = "active"


class MonitorRecordRead(BaseModel):
    id: int
    name: str
    status: str
    created_at: datetime

    model_config = {"from_attributes": True}


class StageCreate(BaseModel):
    name: str
    description: Optional[str] = None
    handler: str
    input_type: str
    output_type: str
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    model_name: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    dependencies: Optional[Dict[str, Any]] = None
    runtime_env: Optional[Dict[str, Any]] = None
    can_split: bool = False
    is_deployable: bool = True


class StageRead(BaseModel):
    name: str
    description: Optional[str]
    handler: str
    input_type: str
    output_type: str
    input_schema: Optional[Dict[str, Any]]
    output_schema: Optional[Dict[str, Any]]
    model_name: Optional[str]
    config: Optional[Dict[str, Any]]
    dependencies: Optional[Dict[str, Any]]
    runtime_env: Optional[Dict[str, Any]]
    can_split: bool
    is_deployable: bool
    created_at: datetime

    model_config = {"from_attributes": True}


class DeploymentConfigCreate(BaseModel):
    stage_name: str
    allowed_tiers: List[str]
    resources: Dict[str, Any]
    replicas: int = 1
    node_affinity: Optional[Dict[str, Any]] = None
    proximity: Optional[Dict[str, Any]] = None
    description: Optional[str] = None


class DeploymentConfigRead(BaseModel):
    stage_name: str
    allowed_tiers: List[str]
    resources: Dict[str, Any]
    replicas: int
    node_affinity: Optional[Dict[str, Any]]
    proximity: Optional[Dict[str, Any]]
    description: Optional[str]
    created_at: datetime

    model_config = {"from_attributes": True}


class StrategyCreate(BaseModel):
    name: str
    strategy_type: str
    handler: str
    config: Optional[Dict[str, Any]] = None
    description: Optional[str] = None


class StrategyRead(BaseModel):
    name: str
    strategy_type: str
    handler: str
    config: Optional[Dict[str, Any]]
    description: Optional[str]
    created_at: datetime

    model_config = {"from_attributes": True}


class EdgeCreate(BaseModel):
    from_stage: str
    to_stage: str
    condition: Optional[str] = None
    weight: Optional[float] = None
    is_split_point: bool = False


class ApplicationCreate(BaseModel):
    name: str
    description: Optional[str] = None
    input_type: str
    stages: List[str]
    edges: List[EdgeCreate]
    entry_stage: str
    exit_stages: List[str]


class ApplicationRead(BaseModel):
    app_id: str
    name: str
    description: Optional[str]
    input_type: str
    created_at: datetime

    model_config = {"from_attributes": True}


class TaskCreate(BaseModel):
    app_name: str
    strategy_name: str
    input_data_uri: Optional[str] = None


class TaskRead(BaseModel):
    task_id: str
    app_name: str
    strategy_name: str
    input_data_uri: Optional[str]
    final_output_uri: Optional[str]
    final_output: Optional[Any] = None
    status: str
    created_at: datetime
    completed_at: Optional[datetime]

    model_config = {"from_attributes": True}
    
    @classmethod
    def model_validate(cls, obj, *args, **kwargs):
        instance = super().model_validate(obj, *args, **kwargs)
        # 反序列化 final_output_uri
        if instance.final_output_uri:
            import json
            try:
                instance.final_output = json.loads(instance.final_output_uri)
            except (json.JSONDecodeError, TypeError):
                instance.final_output = instance.final_output_uri
        return instance


class ExecutionTraceRead(BaseModel):
    trace_id: int
    task_id: str
    step_index: int
    stage_name: str
    node_id: Optional[str]
    node_tier: Optional[str]
    start_time: Optional[datetime]
    execute_time: Optional[datetime] = None
    end_time: Optional[datetime]
    queue_time_ms: Optional[float] = None
    actual_exec_time_ms: Optional[float] = None
    execution_time_ms: Optional[float]
    transfer_time_ms: Optional[float]
    input_size_bytes: Optional[int]
    output_size_bytes: Optional[int]
    cpu_percent: Optional[float]
    memory_mb: Optional[int]
    error_msg: Optional[str]

    model_config = {"from_attributes": True}


class ServiceCreate(BaseModel):
    name: str
    prefix: Optional[str] = None
    file_path: Optional[str] = None
    num_cpus: float = 0.5
    num_memory: int = 128
    max_replicas: int = 10
    min_replicas: int = 1
    description: Optional[str] = None


class ServiceRead(BaseModel):
    id: int
    name: str
    prefix: Optional[str] = None
    file_path: Optional[str] = None
    num_cpus: float
    num_memory: int
    max_replicas: int
    min_replicas: int
    description: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class ServiceUpdate(BaseModel):
    name: Optional[str] = None
    prefix: Optional[str] = None
    file_path: Optional[str] = None
    num_cpus: Optional[float] = None
    num_memory: Optional[int] = None
    max_replicas: Optional[int] = None
    min_replicas: Optional[int] = None
    description: Optional[str] = None
