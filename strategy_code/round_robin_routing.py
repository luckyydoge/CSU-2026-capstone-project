from typing import Dict, Any, List


class RoundRobinRouting:
    """
    轮询路由策略
    
    按顺序循环选择下一阶段和逻辑层级
    """
    
    def __init__(self):
        self.stage_index = 0
        self.tier_index = 0
        self.tiers = ["end", "edge", "cloud"]
    
    def decide(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        轮询路由决策
        
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
        
        # 如果没有下一阶段，终止执行
        if not possible_next:
            return {
                "next_stage": None,
                "target_tier": "edge",
                "should_terminate": True
            }
        
        # 轮询选择下一阶段
        next_stage = possible_next[self.stage_index % len(possible_next)]
        self.stage_index += 1
        
        # 轮询选择逻辑层级
        target_tier = self.tiers[self.tier_index % len(self.tiers)]
        self.tier_index += 1
        
        return {
            "next_stage": next_stage,
            "target_tier": target_tier,
            "should_terminate": False
        }


# 创建全局实例
round_routing = RoundRobinRouting()


def decide(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    轮询路由策略入口函数
    
    从可能的下一阶段中按顺序循环选择，逻辑层级也按端/边/云循环
    
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
    return round_routing.decide(context)
