from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, Literal

StrategyType = Literal["routing", "split", "fallback"]

class StrategyCreateRequest(BaseModel):
    name: str = Field(..., description="策略唯一名称")
    strategy_type: StrategyType = Field(..., description="策略类型：routing/split/fallback")
    handler: str = Field(..., description="代码入口，如 'strategies.random_routing:decide' 或 'strategies.threshold:ThresholdRouting'")
    config: Dict[str, Any] = Field(default_factory=dict, description="策略静态配置参数")
    description: Optional[str] = Field(None, description="策略描述")