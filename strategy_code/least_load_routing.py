from typing import Dict, Any


def decide(context: Dict[str, Any], config: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    最低负载路由策略。

    在目标层级（默认 edge）中，选择 CPU+内存占用最低的节点执行。
    策略可通过 config 指定 target_tier，默认为 edge。

    config 参数:
        target_tier: 目标层级 ("end" | "edge" | "cloud")，默认 "edge"
    """
    possible_next = context.get("possible_next_stages", [])
    if not possible_next:
        return {"next_stage": None, "should_terminate": True}

    nodes = context.get("available_nodes", [])
    cfg = config or {}
    target_tier = cfg.get("target_tier", "edge")

    # 筛选目标层级的节点
    tier_nodes = [n for n in nodes if n.get("tier") == target_tier]

    if not tier_nodes:
        return {
            "next_stage": possible_next[0],
            "target_tier": target_tier,
            "target_node": None,
            "should_terminate": False,
        }

    # 按 CPU×0.5 + 内存×0.5 加权评分，选最低负载
    def load_score(n):
        cpu = n.get("current_cpu_percent") or 0
        mem = n.get("current_memory_percent") or 0
        return cpu * 0.5 + mem * 0.5

    healthy = [n for n in tier_nodes if (n.get("current_cpu_percent") or 0) < 95]
    if not healthy:
        healthy = tier_nodes

    best = min(healthy, key=load_score)

    return {
        "next_stage": possible_next[0],
        "target_tier": target_tier,
        "target_node": best.get("node_id"),
        "should_terminate": False,
    }
