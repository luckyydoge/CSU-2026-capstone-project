from typing import Dict, Optional
from models.strategy import StrategyCreateRequest
from app.database import SessionLocal
from app.schemas import StrategyCreate
from app.models import Strategy
from sqlalchemy.orm import Session


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class StrategyService:
    # ========== 内部管理 DB 的接口（供 /api/v1/ 使用） ==========
    @staticmethod
    def validate(req: StrategyCreateRequest):
        if not req.name:
            raise ValueError("Strategy name required")
        if not req.strategy_type:
            raise ValueError("Strategy type required")
        if not req.handler:
            raise ValueError("Handler required")
        if ":" not in req.handler:
            raise ValueError("Handler must be in format 'module:function'")

    @staticmethod
    def create_strategy(req: StrategyCreateRequest) -> Dict:
        db_gen = get_db()
        db = next(db_gen)
        try:
            strategy_create = StrategyCreate(
                name=req.name,
                strategy_type=req.strategy_type,
                handler=req.handler,
                config=req.config,
                description=req.description
            )
            return StrategyService._create_strategy_db(db, strategy_create)
        finally:
            db_gen.close()

    @staticmethod
    def get_strategy(name: str) -> Optional[Dict]:
        db_gen = get_db()
        db = next(db_gen)
        try:
            strategy = StrategyService._get_strategy_db(db, name)
            if strategy:
                return {
                    "name": strategy.name,
                    "strategy_type": strategy.strategy_type,
                    "handler": strategy.handler,
                    "config": strategy.config,
                    "description": strategy.description
                }
            return None
        finally:
            db_gen.close()

    @staticmethod
    def list_strategies() -> Dict:
        db_gen = get_db()
        db = next(db_gen)
        try:
            strategies = StrategyService._list_strategies_db(db)
            result = {}
            for name, strategy in strategies.items():
                result[name] = {
                    "name": strategy.name,
                    "strategy_type": strategy.strategy_type,
                    "handler": strategy.handler,
                    "config": strategy.config,
                    "description": strategy.description
                }
            return result
        finally:
            db_gen.close()
    
    @staticmethod
    def update_strategy(name: str, req: StrategyCreateRequest) -> Dict:
        db_gen = get_db()
        db = next(db_gen)
        try:
            strategy_create = StrategyCreate(
                name=req.name,
                strategy_type=req.strategy_type,
                handler=req.handler,
                config=req.config,
                description=req.description
            )
            return StrategyService._update_strategy_db(db, name, strategy_create)
        finally:
            db_gen.close()
    
    @staticmethod
    def delete_strategy(name: str) -> Dict:
        db_gen = get_db()
        db = next(db_gen)
        try:
            return StrategyService._delete_strategy_db(db, name)
        finally:
            db_gen.close()
    
    # ========== 外部传入 DB 的接口（供 /db/v1/ 使用） ==========
    @staticmethod
    def _validate_db(req: StrategyCreate):
        if not req.name:
            raise ValueError("Strategy name required")
        if not req.strategy_type:
            raise ValueError("Strategy type required")
        if not req.handler:
            raise ValueError("Handler required")
        if ":" not in req.handler:
            raise ValueError("Handler must be in format 'module:function'")
    
    @staticmethod
    def _create_strategy_db(db: Session, req: StrategyCreate):
        StrategyService._validate_db(req)
        
        existing = db.query(Strategy).filter(Strategy.name == req.name).first()
        if existing:
            raise ValueError(f"Strategy already exists: {req.name}")
        
        strategy = Strategy(
            name=req.name,
            strategy_type=req.strategy_type,
            handler=req.handler,
            config=req.config,
            description=req.description
        )
        
        db.add(strategy)
        db.commit()
        db.refresh(strategy)
        
        return {
            "strategy_name": strategy.name,
            "message": "Strategy registered successfully"
        }
    
    @staticmethod
    def _get_strategy_db(db: Session, name: str) -> Optional[Strategy]:
        return db.query(Strategy).filter(Strategy.name == name).first()
    
    @staticmethod
    def _list_strategies_db(db: Session) -> Dict:
        strategies = db.query(Strategy).all()
        return {strategy.name: strategy for strategy in strategies}
    
    @staticmethod
    def _update_strategy_db(db: Session, name: str, req: StrategyCreate):
        existing = db.query(Strategy).filter(Strategy.name == name).first()
        if not existing:
            raise ValueError(f"Strategy not found: {name}")
        
        StrategyService._validate_db(req)
        
        existing.strategy_type = req.strategy_type
        existing.handler = req.handler
        existing.config = req.config
        existing.description = req.description
        
        db.commit()
        db.refresh(existing)
        
        return {
            "strategy_name": existing.name,
            "message": "Strategy updated successfully"
        }
    
    @staticmethod
    def _delete_strategy_db(db: Session, name: str):
        strategy = db.query(Strategy).filter(Strategy.name == name).first()
        if not strategy:
            raise ValueError(f"Strategy not found: {name}")
        
        db.delete(strategy)
        db.commit()
        
        return {
            "strategy_name": name,
            "message": "Strategy deleted successfully"
        }
