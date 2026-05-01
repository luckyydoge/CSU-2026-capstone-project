from typing import Dict, List, Optional
from app.schemas import StageCreate
from app.models import Stage, Model
from sqlalchemy.orm import Session


class StageService:
    @staticmethod
    def _validate_db(db: Session, req: StageCreate):
        if not req.name:
            raise ValueError("Stage name required")
        if ":" not in req.handler:
            raise ValueError("Handler must be in format 'module:function' (e.g., 'my_stage:run')")
        if not req.input_type:
            raise ValueError("input_type required")
        if not req.output_type:
            raise ValueError("output_type required")
        # 校验 handler 引用的代码文件存在且包含该函数
        import os, importlib.util
        from config import CONFIG
        module_name, func_name = req.handler.split(":", 1)
        file_path = os.path.join(CONFIG.STAGED_CODE_DIR, f"{module_name}.py")
        if not os.path.exists(file_path):
            raise ValueError(f"Handler code file not found: staged_code/{module_name}.py")
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
        # parent_stage 环路检测
        if req.parent_stage:
            visited = {req.name}
            cur = req.parent_stage
            while cur:
                if cur in visited:
                    raise ValueError(f"Cycle detected: parent_stage '{req.parent_stage}' would create a loop")
                visited.add(cur)
                parent = db.query(Stage).filter(Stage.name == cur).first()
                cur = parent.parent_stage if parent else None
        # 校验 model_name 存在（如果填写）
        if req.model_name:
            model_exists = db.query(Model).filter(Model.name == req.model_name).first()
            if not model_exists:
                raise ValueError(f"Model '{req.model_name}' not found. Register the model first or leave model_name empty.")
    
    @staticmethod
    def _create_stage_db(db: Session, req: StageCreate):
        StageService._validate_db(db, req)
        
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
            parent_stage=req.parent_stage,
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
    
    @staticmethod
    def _delete_stage_db(db: Session, name: str):
        from config import CONFIG
        import os
        stage = db.query(Stage).filter(Stage.name == name).first()
        if not stage:
            raise ValueError(f"Stage not found: {name}")
        # 检查是否被应用引用
        from app.models import ApplicationStage, ApplicationEdge, ApplicationEntry, ApplicationExit
        refs = (
            db.query(ApplicationStage).filter(ApplicationStage.stage_name == name).count() +
            db.query(ApplicationEdge).filter(
                (ApplicationEdge.from_stage == name) | (ApplicationEdge.to_stage == name)
            ).count() +
            db.query(ApplicationEntry).filter(ApplicationEntry.stage_name == name).count() +
            db.query(ApplicationExit).filter(ApplicationExit.stage_name == name).count()
        )
        if refs > 0:
            raise ValueError(f"Cannot delete stage '{name}': it is referenced by {refs} application(s). Remove the references first.")
        handler = stage.handler
        module_name = handler.split(":")[0]
        file_path = os.path.join(CONFIG.STAGED_CODE_DIR, f"{module_name}.py")
        if os.path.exists(file_path):
            os.remove(file_path)
        from app.models import DeploymentConfig
        db.query(DeploymentConfig).filter(DeploymentConfig.stage_name == name).delete()
        db.delete(stage)
        db.commit()

    @staticmethod
    def _update_stage_db(db: Session, name: str, req: StageCreate):
        StageService._validate_db(db, req)
        stage = db.query(Stage).filter(Stage.name == name).first()
        if not stage:
            raise ValueError(f"Stage not found: {name}")
        stage.description = req.description
        stage.handler = req.handler
        stage.input_type = req.input_type
        stage.output_type = req.output_type
        stage.input_schema = req.input_schema
        stage.output_schema = req.output_schema
        stage.model_name = req.model_name
        stage.config = req.config
        stage.dependencies = req.dependencies
        stage.runtime_env = req.runtime_env
        stage.parent_stage = req.parent_stage
        stage.can_split = req.can_split
        stage.is_deployable = req.is_deployable
        db.commit()
        db.refresh(stage)
