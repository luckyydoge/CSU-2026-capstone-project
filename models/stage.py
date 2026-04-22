from pydantic import BaseModel, Field
from typing import Dict, Optional, List

class StageCreateRequest(BaseModel):
    name: str = Field(..., description="阶段唯一名称")

    # 基础信息
    description: Optional[str] = Field(None, description="功能描述")

    # 执行入口：支持两种格式
    # 1. "module.function"   -> 从模块导入函数
    # 2. "module:ClassName"  -> 从模块导入可调用类实例
    handler: str = Field(..., description="执行入口，例如 'my_stages.preprocess:run' 或 'my_stages.light_inference:LightInference'")

    # 输入输出类型（字符串，用于快速匹配和策略决策）
    input_type: str = Field(..., description="输入数据类型，如 'image', 'video', 'json', 'feature'")
    output_type: str = Field(..., description="输出数据类型")

    # 详细 schema（可选，用于复杂校验）
    input_schema: Dict = Field(default_factory=dict, description="详细输入 JSON Schema")
    output_schema: Dict = Field(default_factory=dict, description="详细输出 JSON Schema")

    # 模型依赖（引用已注册的模型名称）
    model_name: Optional[str] = Field(None, description="关联的模型名称")

    # 参数配置
    config: Dict = Field(default_factory=dict, description="阶段特定的配置参数")

    # 运行环境依赖
    dependencies: List[str] = Field(default_factory=list, description="Python 依赖包列表，如 ['torch', 'opencv-python']")
    runtime_env: Dict = Field(default_factory=dict, description="Ray 运行时环境配置（高级）")

    # 可扩展性标志
    can_split: bool = Field(False, description="是否支持模型级切分（进一步拆分成子阶段）")
    is_deployable: bool = Field(True, description="是否可独立部署到某个节点")