from typing import Dict, Any


def decide(context: Dict[str, Any]) -> Dict[str, Any]:
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
