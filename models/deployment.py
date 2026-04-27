from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Literal

Tier = Literal["end", "edge", "cloud"]

class DeploymentConfigCreateRequest(BaseModel):
    stage_name: str = Field(..., description="阶段名称（必须已注册）")
    allowed_tiers: List[Tier] = Field(..., min_items=1)
    resources: Dict[str, Any] = Field(default_factory=lambda: {"cpu_cores": 0.1, "memory_mb": 128})
    replicas: int = Field(1, ge=1)
    node_affinity: Optional[Dict[str, Any]] = None
    proximity: Optional[Dict[str, Any]] = None
    description: Optional[str] = None