import importlib
from typing import Dict, Any, Callable

def get_db():
    """获取数据库Session"""
    from app.database import SessionLocal
    return SessionLocal()

def load_strategy(strategy_name: str) -> Callable:
    """动态加载策略，支持 module.function 或 module:function 格式，也支持 module:ClassName（类需要实现 decide 方法）"""
    # 从数据库加载策略
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

    # 解析 handler: 支持 "module:func" 或 "module.func" 或 "module:Class"
    if ":" in handler:
        module_path, entry = handler.split(":", 1)
    else:
        module_path, entry = handler.rsplit(".", 1)

    module = importlib.import_module(module_path)

    # 尝试获取属性
    obj = getattr(module, entry)

    # 判断是可调用类还是函数
    if callable(obj) and not isinstance(obj, type):
        # 函数形式，包装 config
        def wrapped(context):
            return obj(context, config)
        return wrapped
    elif isinstance(obj, type):
        # 类形式，实例化并返回 decide 方法
        instance = obj(config)
        if hasattr(instance, "decide"):
            return instance.decide
        else:
            raise ValueError(f"Class {entry} does not have 'decide' method")
    else:
        raise ValueError(f"Handler {handler} does not point to a callable function or class")
