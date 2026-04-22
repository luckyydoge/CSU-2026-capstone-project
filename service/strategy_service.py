from typing import Dict, Optional
from models.strategy import StrategyCreateRequest
from storage.memory_store import STRATEGY_DB

class StrategyService:

    @staticmethod
    def validate(req: StrategyCreateRequest):
        if not req.name or not req.name.strip():
            raise ValueError("策略名称不能为空")
        if req.strategy_type not in ("routing", "split", "fallback"):
            raise ValueError("strategy_type 必须是 routing, split 或 fallback")
        if not req.handler or ('.' not in req.handler and ':' not in req.handler):
            raise ValueError("handler 格式错误，应为 module.function 或 module:ClassName")

    @staticmethod
    def create_strategy(req: StrategyCreateRequest) -> Dict:
        StrategyService.validate(req)
        if req.name in STRATEGY_DB:
            raise ValueError(f"策略 '{req.name}' 已存在")
        STRATEGY_DB[req.name] = req.dict()
        return {
            "strategy_name": req.name,
            "message": "Strategy registered successfully"
        }

    @staticmethod
    def get_strategy(name: str) -> Optional[Dict]:
        return STRATEGY_DB.get(name)

    @staticmethod
    def list_strategies() -> Dict:
        return STRATEGY_DB

    @staticmethod
    def update_strategy(name: str, req: StrategyCreateRequest) -> Dict:
        if name not in STRATEGY_DB:
            raise ValueError(f"策略 '{name}' 不存在")
        if req.name != name:
            raise ValueError("请求中的策略名称与路径参数不匹配")
        StrategyService.validate(req)
        STRATEGY_DB[name] = req.dict()
        return {
            "strategy_name": name,
            "message": "Strategy updated successfully"
        }

    @staticmethod
    def delete_strategy(name: str) -> Dict:
        if name not in STRATEGY_DB:
            raise ValueError(f"策略 '{name}' 不存在")
        del STRATEGY_DB[name]
        return {
            "strategy_name": name,
            "message": "Strategy deleted successfully"
        }