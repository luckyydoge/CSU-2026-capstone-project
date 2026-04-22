from pydantic import BaseModel, Field
from typing import List, Dict, Optional

class StageSchema(BaseModel):
    name: str = Field(..., description="阶段名称，必须已在系统中注册")
    output_type: str = Field(..., description="该阶段的输出类型，用于数据流校验")

class EdgeSchema(BaseModel):
    from_stage: str = Field(..., description="源阶段名称")
    to_stage: str = Field(..., description="目标阶段名称")
    # 可选：条件、权重、是否切分点等（后续扩展）
    condition: Optional[str] = None
    is_split_point: bool = False

class ApplicationCreateRequest(BaseModel):
    name: str = Field(..., description="应用名称，唯一")
    description: Optional[str] = Field(None, description="应用描述")
    input_type: str = Field(..., description="应用整体输入类型，如 image/video/json")
    stages: List[StageSchema] = Field(..., description="应用包含的阶段（引用已注册阶段）")
    edges: List[EdgeSchema] = Field(..., description="阶段之间的有向边")
    entry_stage: str = Field(..., description="入口阶段名称")
    exit_stages: List[str] = Field(..., description="出口阶段名称列表")

class ApplicationResponse(BaseModel):
    app_id: str  # 或使用 name 作为 ID
    name: str
    description: Optional[str]
    input_type: str
    stages: List[StageSchema]
    edges: List[EdgeSchema]
    entry_stage: str
    exit_stages: List[str]