from typing import Dict, Any


def simple_split_decide(context: Dict[str, Any], config: Dict[str, Any] = None) -> Dict[str, Any]:
    possible_next = context.get("possible_next_stages", [])
    if not possible_next:
        return {"next_stage": None, "should_terminate": True}
    is_split = context.get("is_split_point", False) and context.get("stage_can_split", False)
    result = {
        "next_stage": possible_next[0],
        "target_tier": "edge",
        "should_terminate": False,
    }
    if is_split:
        result["split_plan"] = {"count": 2, "method": "even_split"}
    return result


def simple_fallback_decide(context: Dict[str, Any], error_info: Dict[str, Any] = None, config: Dict[str, Any] = None) -> Dict[str, Any]:
    if error_info:
        print(f"[示例回退] 阶段 {error_info.get('stage')} 失败: {error_info.get('error')}")
        return {"action": "skip", "next_stage": context.get("possible_next_stages", [None])[0]}
    possible_next = context.get("possible_next_stages", [])
    if possible_next:
        return {"next_stage": possible_next[0], "target_tier": "edge"}
    return {"should_terminate": True}
