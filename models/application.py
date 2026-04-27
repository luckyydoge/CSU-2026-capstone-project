from pydantic import BaseModel, Field
from typing import List, Dict, Optional

class EdgeSchema(BaseModel):
    from_stage: str = Field(..., description="源阶段名称")
    to_stage: str = Field(..., description="目标阶段名称")
    condition: Optional[str] = None
    is_split_point: bool = False

class ApplicationCreateRequest(BaseModel):
    name: str = Field(..., description="应用名称，唯一")
    description: Optional[str] = Field(None, description="应用描述")
    input_type: str = Field(..., description="应用整体输入类型，如 image/video/json")
    stages: List[str] = Field(..., description="应用包含的阶段名称列表")
    edges: List[EdgeSchema] = Field(..., description="阶段之间的有向边")
    entry_stage: str = Field(..., description="入口阶段名称")
    exit_stages: List[str] = Field(..., description="出口阶段名称列表")