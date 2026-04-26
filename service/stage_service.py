# service/stage_service.py
import uuid
from typing import Dict, List, Optional
from models.stage import StageCreateRequest
from app.database import SessionLocal
from app.schemas import StageCreate
from app.models import Stage
from sqlalchemy.orm import Session


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class StageService:
    # ========== 内部管理 DB 的接口（供 /api/v1/ 使用） ==========
    @staticmethod
    def validate(req: StageCreateRequest):
        if not req.name:
            raise ValueError("Stage name required")
        if ":" not in req.handler:
            raise ValueError("Handler must be in format 'module:function' (e.g., 'my_stage:run')")
        if not req.input_type:
            raise ValueError("input_type 不能为空")
        if not req.output_type:
            raise ValueError("output_type 不能为空")

    @staticmethod
    def create_stage(req: StageCreateRequest):
        StageService.validate(req)
        
        db_gen = get_db()
        db = next(db_gen)
        try:
            stage_create = StageCreate(
                name=req.name,
                description=req.description,
                handler=req.handler,
                input_type=req.input_type,
                output_type=req.output_type,
                input_schema=req.input_schema,
                output_schema=req.output_schema,
                model_name=req.model_name,
                config=req.config,
                dependencies=dict.fromkeys(req.dependencies, None),
                runtime_env=req.runtime_env,
                can_split=req.can_split,
                is_deployable=req.is_deployable
            )
            return StageService._create_stage_db(db, stage_create)
        finally:
            db_gen.close()

    @staticmethod
    def get_stage(name: str) -> Optional[Dict]:
        db_gen = get_db()
        db = next(db_gen)
        try:
            stage = StageService._get_stage_db(db, name)
            if stage:
                return {
                    "name": stage.name,
                    "description": stage.description,
                    "handler": stage.handler,
                    "input_type": stage.input_type,
                    "output_type": stage.output_type,
                    "input_schema": stage.input_schema,
                    "output_schema": stage.output_schema,
                    "model_name": stage.model_name,
                    "config": stage.config,
                    "dependencies": stage.dependencies,
                    "runtime_env": stage.runtime_env,
                    "can_split": stage.can_split,
                    "is_deployable": stage.is_deployable
                }
            return None
        finally:
            db_gen.close()

    @staticmethod
    def list_stages() -> Dict:
        db_gen = get_db()
        db = next(db_gen)
        try:
            stages = StageService._list_stages_db(db)
            result = {}
            for name, stage in stages.items():
                result[name] = {
                    "name": stage.name,
                    "description": stage.description,
                    "handler": stage.handler,
                    "input_type": stage.input_type,
                    "output_type": stage.output_type,
                    "input_schema": stage.input_schema,
                    "output_schema": stage.output_schema,
                    "model_name": stage.model_name,
                    "config": stage.config,
                    "dependencies": stage.dependencies,
                    "runtime_env": stage.runtime_env,
                    "can_split": stage.can_split,
                    "is_deployable": stage.is_deployable
                }
            return result
        finally:
            db_gen.close()
    
    # ========== 外部传入 DB 的接口（供 /db/v1/ 使用） ==========
    @staticmethod
    def _validate_db(req: StageCreate):
        if not req.name:
            raise ValueError("Stage name required")
        if ":" not in req.handler:
            raise ValueError("Handler must be in format 'module:function' (e.g., 'my_stage:run')")
        if not req.input_type:
            raise ValueError("input_type required")
        if not req.output_type:
            raise ValueError("output_type required")
    
    @staticmethod
    def _create_stage_db(db: Session, req: StageCreate):
        StageService._validate_db(req)
        
        existing = db.query(Stage).filter(Stage.name == req.name).first()
        if existing:
            raise ValueError(f"Stage already exists: {req.name}")
        
        stage = Stage(
            name=req.name,
            description=req.description,
            handler=req.handler,
            input_type=req.input_type,
            output_type=req.output_type,
            input_schema=req.input_schema,
            output_schema=req.output_schema,
            model_name=req.model_name,
            config=req.config,
            dependencies=req.dependencies,
            runtime_env=req.runtime_env,
            can_split=req.can_split,
            is_deployable=req.is_deployable
        )
        
        db.add(stage)
        db.commit()
        db.refresh(stage)
        
        return {
            "stage_name": stage.name,
            "message": "Stage registered successfully"
        }
    
    @staticmethod
    def _get_stage_db(db: Session, name: str) -> Optional[Stage]:
        return db.query(Stage).filter(Stage.name == name).first()
    
    @staticmethod
    def _list_stages_db(db: Session) -> Dict:
        stages = db.query(Stage).all()
        return {stage.name: stage for stage in stages}
