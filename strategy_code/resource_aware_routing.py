from typing import Dict, Any


def decide(context: Dict[str, Any], config: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    资源感知路由策略。

    优先保持与上一阶段同层级执行（减少传输开销），
    仅当同层级无健康节点时，才按 cloud → edge → end 迁移。

    健康判定：CPU 和内存使用率均低于阈值（默认 80%）。
    """
    possible_next = context.get("possible_next_stages", [])
    if not possible_next:
        return {"next_stage": None, "should_terminate": True}

    nodes = context.get("available_nodes", [])

    cpu_threshold = (config or {}).get("cpu_threshold", 80)
    mem_threshold = (config or {}).get("mem_threshold", 80)

    # 按 tier 分组健康节点
    healthy_by_tier: Dict[str, list] = {}
    for n in nodes:
        cpu = n.get("current_cpu_percent")
        mem = n.get("current_memory_percent")
        if (cpu is None or cpu < cpu_threshold) and (mem is None or mem < mem_threshold):
            tier = n.get("tier", "edge")
            healthy_by_tier.setdefault(tier, []).append(n)

    # 获取上一阶段的执行层级
    history = context.get("execution_history", [])
    prev_tier = history[-1].get("node_tier") if history else None

    # 决策目标层级
    if prev_tier and prev_tier in healthy_by_tier:
        # 同层级有健康节点 → 留在该层级
        target_tier = prev_tier
    else:
        # 同层级无健康节点 → 按优先级选其他层级
        for t in ["cloud", "edge", "end"]:
            if healthy_by_tier.get(t):
                target_tier = t
                break
        else:
            target_tier = "edge"

    return {
        "next_stage": possible_next[0],
        "target_tier": target_tier,
        "should_terminate": False,
    }
