from typing import Dict, Any


def decide(context: Dict[str, Any], config: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    阈值回退路由策略。

    优先在上一阶段的同层级执行（减少数据传输开销）。
    当同层级所有节点的 CPU 或内存超过阈值时，按 cloud → edge → end 顺序
    迁移到其他层级的最优节点。

    config 参数:
        cpu_threshold: CPU 使用率阈值（百分比），默认 80
        mem_threshold: 内存使用率阈值（百分比），默认 80
    """
    possible_next = context.get("possible_next_stages", [])
    if not possible_next:
        return {"next_stage": None, "should_terminate": True}

    nodes = context.get("available_nodes", [])
    cfg = config or {}
    cpu_threshold = cfg.get("cpu_threshold", 80)
    mem_threshold = cfg.get("mem_threshold", 80)

    # 获取上一阶段的执行层级
    history = context.get("execution_history", [])
    prev_tier = history[-1].get("node_tier") if history else None

    def load_score(n):
        cpu = n.get("current_cpu_percent") or 0
        mem = n.get("current_memory_percent") or 0
        return cpu * 0.5 + mem * 0.5

    def best_in_tier(tier):
        tier_nodes = [n for n in nodes if n.get("tier") == tier]
        healthy = [n for n in tier_nodes
                   if (n.get("current_cpu_percent") or 0) < cpu_threshold
                   and (n.get("current_memory_percent") or 0) < mem_threshold]
        if not healthy:
            return None, None
        best = min(healthy, key=load_score)
        return best, tier

    # 先尝试同层级（减少传输开销）
    if prev_tier:
        best_node, tier = best_in_tier(prev_tier)
        if best_node:
            return {
                "next_stage": possible_next[0],
                "target_tier": tier,
                "target_node": best_node.get("node_id"),
                "should_terminate": False,
            }

    # 同层级无健康节点 → 按优先级迁移到其他层级
    for fallback_tier in ["cloud", "edge", "end"]:
        if fallback_tier == prev_tier:
            continue
        best_node, tier = best_in_tier(fallback_tier)
        if best_node:
            print(f"[阈值回退] {prev_tier} 过载，迁移到 {tier}")
            return {
                "next_stage": possible_next[0],
                "target_tier": tier,
                "target_node": best_node.get("node_id"),
                "should_terminate": False,
            }

    # 所有层级都过载 → 回退到默认层级
    fallback = prev_tier or "edge"
    return {
        "next_stage": possible_next[0],
        "target_tier": fallback,
        "target_node": None,
        "should_terminate": False,
    }
