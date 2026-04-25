import random
from typing import Dict, Any, List


def decide(context: Dict[str, Any], config: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    随机路由策略
    
    从可能的下一阶段中随机选择一个执行
    
    Args:
        context: 策略上下文，包含：
            - current_stage: 当前阶段名称
            - input: 当前输入数据
            - possible_next_stages: 可能的下一阶段列表
            - execution_history: 执行历史记录
        config: 策略配置（可选）
    
    Returns:
        决策结果，包含：
            - next_stage: 选择的下一阶段
            - target_tier: 目标逻辑层级（默认为edge）
            - should_terminate: 是否终止执行
    """
    possible_next = context.get("possible_next_stages", [])
    
    # 如果没有下一阶段，终止执行
    if not possible_next:
        return {
            "next_stage": None,
            "target_tier": "edge",
            "should_terminate": True
        }
    
    # 随机选择一个下一阶段
    next_stage = random.choice(possible_next)
    
    # 暂时返回默认层级，由RayExecutor根据部署配置进行验证和调整
    return {
        "next_stage": next_stage,
        "target_tier": "edge",
        "should_terminate": False
    }
