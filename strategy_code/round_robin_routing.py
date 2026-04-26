from typing import Dict, Any


def decide(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    轮询路由策略 — 无状态版本

    根据执行历史的步数决定阶段和层级选择，不依赖全局状态，
    因此并发调用是安全的。

    Args:
        context: 策略上下文，包含：
            - current_stage: 当前阶段名称
            - input: 当前输入数据
            - possible_next_stages: 可能的下一阶段列表
            - execution_history: 执行历史记录

    Returns:
        决策结果，包含：
            - next_stage: 选择的下一阶段
            - target_tier: 目标逻辑层级
            - should_terminate: 是否终止执行
    """
    possible_next = context.get("possible_next_stages", [])
    execution_history = context.get("execution_history", [])
    tiers = ["end", "edge", "cloud"]

    if not possible_next:
        return {
            "next_stage": None,
            "target_tier": "edge",
            "should_terminate": True
        }

    step = len(execution_history)
    next_stage = possible_next[step % len(possible_next)]
    target_tier = tiers[step % len(tiers)]

    return {
        "next_stage": next_stage,
        "target_tier": target_tier,
        "should_terminate": False
    }
