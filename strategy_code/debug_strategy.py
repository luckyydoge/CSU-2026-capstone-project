from typing import Dict, Any

def debug_strategy_end(context: Dict[str, Any], config: Dict[str, Any] = None) -> Dict[str, Any]:
    possible_next = context.get("possible_next_stages", [])
    if possible_next:
        return {
            "next_stage": possible_next[0],
            "target_tier": "end"
        }
    return {"should_terminate": True}

def debug_strategy_edge(context: Dict[str, Any], config: Dict[str, Any] = None) -> Dict[str, Any]:
    possible_next = context.get("possible_next_stages", [])
    if possible_next:
        return {
            "next_stage": possible_next[0],
            "target_tier": "edge"
        }
    return {"should_terminate": True}

def debug_strategy_cloud(context: Dict[str, Any], config: Dict[str, Any] = None) -> Dict[str, Any]:
    possible_next = context.get("possible_next_stages", [])
    if possible_next:
        return {
            "next_stage": possible_next[0],
            "target_tier": "cloud"
        }
    return {"should_terminate": True}
