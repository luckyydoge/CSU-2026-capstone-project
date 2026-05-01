import importlib
import sys
from typing import Dict, Any, Optional


class StrategyProxy:
    """统一策略代理，包装函数式或类式策略，按场景调用对应方法。"""

    def __init__(self, decide_fn=None, fallback_fn=None, split_fn=None, config=None):
        self._decide = decide_fn
        self._fallback = fallback_fn
        self._split = split_fn
        self.config = config or {}

    def decide(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """路径决策：返回 next_stage, target_tier, should_terminate 等。"""
        if self._decide:
            return self._decide(context, self.config)
        raise NotImplementedError("Strategy has no 'decide' method")

    def decide_fallback(self, context: Dict[str, Any], error_info: Dict[str, Any]) -> Dict[str, Any]:
        """回退决策：返回 action("skip"/"retry"/"terminate"), next_stage 等。"""
        if self._fallback:
            return self._fallback(context, error_info, self.config)
        return {"action": "terminate"}

    def decide_split(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """切分决策：返回 split_plan, next_stage, target_tier 等。"""
        if self._split:
            return self._split(context, self.config)
        raise NotImplementedError("Strategy has no 'decide_split' method")

    @property
    def has_fallback(self) -> bool:
        return self._fallback is not None

    @property
    def has_split(self) -> bool:
        return self._split is not None


def load_strategy(strategy_name: str, db=None) -> StrategyProxy:
    """动态加载策略，返回 StrategyProxy 对象，可选传入已有 db session。"""
    own_db = False
    if db is None:
        from app.database import SessionLocal
        db = SessionLocal()
        own_db = True
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
        if own_db:
            db.close()

    if not strategy_info:
        raise ValueError(f"Strategy '{strategy_name}' not found")

    handler = strategy_info["handler"]
    config = strategy_info.get("config", {}) or {}

    if ":" in handler:
        module_path, entry = handler.split(":", 1)
    elif "." in handler:
        module_path, entry = handler.rsplit(".", 1)
    else:
        raise ValueError(f"Handler '{handler}' must be in 'module:function' or 'module.function' format")

    # 清除模块缓存，支持热加载（修改代码后不需重启）
    if module_path in sys.modules:
        del sys.modules[module_path]
        importlib.invalidate_caches()

    module = importlib.import_module(module_path)
    obj = getattr(module, entry)

    if callable(obj) and not isinstance(obj, type):
        # 函数式策略：handler 指向的函数作为 decide
        # 同模块的 decide_fallback / decide_split 作为可选补充
        proxy = StrategyProxy(
            decide_fn=lambda ctx, cfg: obj(ctx, cfg),
            fallback_fn=getattr(module, 'decide_fallback', None),
            split_fn=getattr(module, 'decide_split', None),
            config=config,
        )
        return proxy
    elif isinstance(obj, type):
        # 类式策略：实例化后提取所有存在的方法
        instance = obj(config)
        proxy = StrategyProxy(config=config)
        if hasattr(instance, 'decide'):
            proxy._decide = instance.decide
        if hasattr(instance, 'decide_fallback'):
            proxy._fallback = instance.decide_fallback
        if hasattr(instance, 'decide_split'):
            proxy._split = instance.decide_split
        if not (proxy._decide or proxy._fallback or proxy._split):
            raise ValueError(f"Class {entry} has no 'decide'/'decide_fallback'/'decide_split' method")
        return proxy
    else:
        raise ValueError(f"Handler {handler} does not point to a callable function or class")
