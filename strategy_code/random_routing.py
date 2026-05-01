import random
from typing import Dict, Any

def decide(context: Dict[str, Any], config: Dict[str, Any] = None) -> Dict[str, Any]:
    if config and "seed" in config:
        random.seed(config["seed"])
    possible = context.get("possible_next_stages", [])
    if not possible:
        return {"next_stage": None, "target_tier": None, "should_terminate": True}
    next_stage = random.choice(possible)
    tiers = ["end", "edge", "cloud"]
    target_tier = random.choice(tiers)
    return {"next_stage": next_stage, "target_tier": target_tier, "should_terminate": False}
