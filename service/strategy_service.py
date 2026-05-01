from typing import Dict, Optional
from app.schemas import StrategyCreate
from app.models import Strategy
from sqlalchemy.orm import Session


class StrategyService:
    # ========== 外部传入 DB 的接口 ==========
    @staticmethod
    def _validate_db(req: StrategyCreate):
        if not req.name:
            raise ValueError("Strategy name required")
        if not req.strategy_type:
            req.strategy_type = "routing"
        if not req.handler:
            raise ValueError("Handler required")
        if ":" not in req.handler:
            raise ValueError("Handler must be in format 'module:function'")
        # 校验 handler 引用的代码文件存在且包含该函数
        import os, importlib.util
        from config import CONFIG
        module_name, func_name = req.handler.split(":", 1)
        file_path = os.path.join(CONFIG.STRATEGY_CODE_DIR, f"{module_name}.py")
        if not os.path.exists(file_path):
            raise ValueError(f"Handler code file not found: strategy_code/{module_name}.py")
        try:
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            if not hasattr(module, func_name):
                raise ValueError(f"Function '{func_name}' not found in module '{module_name}'")
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Failed to load handler module: {e}")
    
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
        from config import CONFIG
        import os
        strategy = db.query(Strategy).filter(Strategy.name == name).first()
        if not strategy:
            raise ValueError(f"Strategy not found: {name}")

        # 检查是否被任务引用
        from app.models import Task
        task_count = db.query(Task).filter(Task.strategy_name == name).count()
        if task_count > 0:
            raise ValueError(f"Cannot delete strategy '{name}': it is referenced by {task_count} task(s).")

        handler = strategy.handler
        module_name = handler.split(":")[0]
        file_path = os.path.join(CONFIG.STRATEGY_CODE_DIR, f"{module_name}.py")
        if os.path.exists(file_path):
            os.remove(file_path)

        db.delete(strategy)
        db.commit()

        return {
            "strategy_name": name,
            "message": "Strategy and code file deleted successfully"
        }
