import importlib
from typing import Dict, Any, Callable, Tuple

def get_db():
    """获取数据库Session"""
    from app.database import SessionLocal
    return SessionLocal()

def load_strategy(strategy_name: str) -> Tuple[Callable, str]:
    """动态加载策略，返回 (callable, strategy_type)"""
    db = get_db()
    strategy_info = None
    try:
        from app.models import Strategy
        strategy = db.query(Strategy).filter(Strategy.name == strategy_name).first()
        if strategy:
            strategy_info = {
                "name": strategy.name,
                "strategy_type": strategy.strategy_type,
                "handler": strategy.handler,
                "config": strategy.config,
                "description": strategy.description
            }
    finally:
        db.close()
    
    if not strategy_info:
        raise ValueError(f"Strategy '{strategy_name}' not found")

    handler = strategy_info["handler"]
    config = strategy_info.get("config", {})

    if ":" in handler:
        module_path, entry = handler.split(":", 1)
    else:
        module_path, entry = handler.rsplit(".", 1)

    module = importlib.import_module(module_path)
    obj = getattr(module, entry)

    if callable(obj) and not isinstance(obj, type):
        def wrapped(context, extra=None):
            if extra:
                return obj(context, extra, config)
            return obj(context, config)
        return wrapped, strategy_info["strategy_type"]
    elif isinstance(obj, type):
        instance = obj(config)
        if hasattr(instance, "decide"):
            return instance.decide, strategy_info["strategy_type"]
        elif hasattr(instance, "decide_split"):
            return instance.decide_split, strategy_info["strategy_type"]
        elif hasattr(instance, "decide_fallback"):
            return instance.decide_fallback, strategy_info["strategy_type"]
        else:
            raise ValueError(f"Class {entry} has no 'decide'/'decide_split'/'decide_fallback' method")
    else:
        raise ValueError(f"Handler {handler} does not point to a callable function or class")
