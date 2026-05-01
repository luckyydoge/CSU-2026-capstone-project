from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, model_validator


class MonitorRecordCreate(BaseModel):
    name: str
    status: str = "active"


class MonitorRecordRead(BaseModel):
    id: int
    name: str
    status: str
    created_at: datetime

    model_config = {"from_attributes": True}


class ModelCreate(BaseModel):
    name: str
    version: str = "1.0"
    stage_name: Optional[str] = None
    weight_path: Optional[str] = None
    load_method: Optional[str] = None
    inference_config: Optional[Dict[str, Any]] = None
    alternative_models: Optional[Dict[str, Any]] = None


class ModelRead(BaseModel):
    model_id: str
    name: str
    version: str
    stage_name: Optional[str]
    weight_path: Optional[str]
    load_method: Optional[str]
    inference_config: Optional[Dict[str, Any]]
    alternative_models: Optional[Dict[str, Any]]
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
    parent_stage: Optional[str] = None
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
    parent_stage: Optional[str]
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
    strategy_type: str = "routing"
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
    input_data: Optional[Any] = None
    runtime_config: Optional[Dict[str, Any]] = None

    @model_validator(mode='before')
    @classmethod
    def convert_input_data(cls, data):
        if isinstance(data, dict):
            input_data = data.get('input_data')
            input_data_uri = data.get('input_data_uri')
            if input_data is not None and not input_data_uri:
                import json
                data['input_data_uri'] = json.dumps(input_data)
                data.pop('input_data', None)
        return data


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

    @model_validator(mode='after')
    @classmethod
    def deserialize_final_output(cls, instance):
        if instance.final_output_uri:
            import json
            try:
                instance.final_output = json.loads(instance.final_output_uri)
            except (json.JSONDecodeError, TypeError):
                instance.final_output = instance.final_output_uri
        return instance


class DataTransformCreate(BaseModel):
    name: str
    input_type: str
    output_type: str
    handler: str
    config: Optional[Dict[str, Any]] = None
    description: Optional[str] = None


class DataTransformRead(BaseModel):
    name: str
    input_type: str
    output_type: str
    handler: str
    config: Optional[Dict[str, Any]]
    description: Optional[str]
    created_at: datetime

    model_config = {"from_attributes": True}


class ExperimentCreate(BaseModel):
    name: str
    app_name: str
    strategy_group: List[str]
    input_dataset: List[Any]
    rounds: int = 1
    max_retries: int = 1
    output_location: Optional[str] = None
    result_method: str = "db"


class ExperimentRead(BaseModel):
    exp_id: str
    name: str
    app_name: str
    strategy_group: List[Any]
    input_dataset: List[Any]
    rounds: int
    max_retries: int = 1
    output_location: Optional[str] = None
    result_method: str = "db"
    status: str
    task_count: Optional[int] = None
    created_at: datetime
    completed_at: Optional[datetime]

    model_config = {"from_attributes": True}


class ExperimentReport(BaseModel):
    exp_id: str
    name: str
    app_name: Optional[str] = None
    status: str
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    pending_tasks: Optional[int] = None
    avg_execution_time_ms: Optional[float]
    total_transfer_time_ms: Optional[float] = None
    avg_cpu_percent: Optional[float] = None
    avg_memory_mb: Optional[float] = None
    strategy_breakdown: List[Dict[str, Any]]
    stage_breakdown: List[Dict[str, Any]]
    tier_breakdown: Optional[List[Dict[str, Any]]] = None
    task_details: Optional[List[Dict[str, Any]]] = None
    errors: Optional[List[Dict[str, Any]]] = None
    created_at: datetime
    completed_at: Optional[datetime]


class ExecutionTraceRead(BaseModel):
    trace_id: int
    task_id: str
    step_index: int
    stage_name: str
    node_id: Optional[str]
    node_tier: Optional[str]
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    execution_time_ms: Optional[float]
    transfer_time_ms: Optional[float]
    input_size_bytes: Optional[int]
    output_size_bytes: Optional[int]
    cpu_percent: Optional[float]
    memory_mb: Optional[int]
    error_msg: Optional[str]

    model_config = {"from_attributes": True}


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
    ray_node_id: Optional[str] = None
    node_ip: Optional[str] = None


class ExecutionTraceSchema(BaseModel):
    task_id: str
    execution_path: List[StepRecord] = []
    total_latency_ms: float = 0.0
    total_transfer_overhead_ms: float = 0.0
    error_logs: List[str] = []
