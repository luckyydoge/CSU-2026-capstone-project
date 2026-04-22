from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Literal

# 逻辑层级类型
Tier = Literal["end", "edge", "cloud"]

class ResourceRequirements(BaseModel):
    cpu_cores: float = Field(0.1, description="所需 CPU 核数，支持小数（如 0.5）")
    memory_mb: int = Field(128, description="所需内存（MB）")
    gpu_count: int = Field(0, description="所需 GPU 数量，0 表示不需要")
    gpu_memory_mb: Optional[int] = Field(None, description="每张 GPU 所需显存（MB）")

class NodeAffinity(BaseModel):
    # 节点标签匹配，例如 {"tier": "edge", "region": "us-east"}
    match_labels: Dict[str, str] = Field(default_factory=dict, description="节点必须具有的标签")
    # 节点名称列表（精确指定，通常用于调试）
    node_names: List[str] = Field(default_factory=list, description="指定节点名称列表（与标签二选一或结合）")

class ProximityRequirement(BaseModel):
    # 邻近部署需求：例如与某个阶段部署在同一节点或同一区域
    target_stage: str = Field(..., description="目标阶段名称")
    proximity_type: Literal["same_node", "same_rack", "same_region"] = Field(..., description="邻近类型")

class DeploymentConfigCreateRequest(BaseModel):
    stage_name: str = Field(..., description="阶段名称（必须已注册）")
    
    # 允许部署的逻辑层级（至少一个）
    allowed_tiers: List[Tier] = Field(..., min_items=1, description="允许部署的逻辑层级列表，如 ['end', 'edge']")
    
    # 资源需求（可选，有默认值）
    resources: ResourceRequirements = Field(default_factory=ResourceRequirements)
    
    # 副本数（该阶段可同时运行的实例数，用于负载均衡）
    replicas: int = Field(1, ge=1, description="副本数")
    
    # 节点选择约束（可选）
    node_affinity: Optional[NodeAffinity] = Field(None, description="节点亲和性约束")
    
    # 邻近部署需求（可选）
    proximity: Optional[ProximityRequirement] = Field(None, description="与某阶段邻近部署的需求")
    
    # 其他元数据
    description: Optional[str] = Field(None, description="部署配置描述")