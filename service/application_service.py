import uuid
from typing import Dict, List, Set, Optional
from collections import defaultdict, deque
from models.application import ApplicationCreateRequest
from app.database import SessionLocal
from app.schemas import ApplicationCreate
from app.models import (
    Application, Stage, ApplicationStage, ApplicationEdge, 
    ApplicationEntry, ApplicationExit
)
from sqlalchemy.orm import Session


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class ApplicationService:
    # ========== 内部管理 DB 的接口（供 /api/v1/ 使用） ==========
    @staticmethod
    def validate(req: ApplicationCreateRequest):
        db_gen = get_db()
        db = next(db_gen)
        try:
            stage_names = set(req.stages)
            for stage_name in stage_names:
                stage = db.query(Stage).filter(Stage.name == stage_name).first()
                if not stage:
                    raise ValueError(f"Stage not found: {stage_name}")
            
            if req.entry_stage not in stage_names:
                raise ValueError(f"Entry stage '{req.entry_stage}' not in stages")
            
            for exit_stage in req.exit_stages:
                if exit_stage not in stage_names:
                    raise ValueError(f"Exit stage '{exit_stage}' not in stages")
            
            for edge in req.edges:
                if edge.from_stage not in stage_names:
                    raise ValueError(f"Edge from_stage '{edge.from_stage}' not in stages")
                if edge.to_stage not in stage_names:
                    raise ValueError(f"Edge to_stage '{edge.to_stage}' not in stages")
            
            graph = defaultdict(list)
            for edge in req.edges:
                graph[edge.from_stage].append(edge.to_stage)
            
            visited = set()
            rec_stack = set()
            
            def has_cycle(node: str) -> bool:
                if node in rec_stack:
                    return True
                if node in visited:
                    return False
                visited.add(node)
                rec_stack.add(node)
                for neighbor in graph.get(node, []):
                    if has_cycle(neighbor):
                        return True
                rec_stack.remove(node)
                return False
            
            if has_cycle(req.entry_stage):
                raise ValueError("Cycle detected, application graph must be DAG")
            
            reachable = set()
            queue = deque([req.entry_stage])
            while queue:
                node = queue.popleft()
                if node in reachable:
                    continue
                reachable.add(node)
                for neighbor in graph.get(node, []):
                    queue.append(neighbor)
            
            unreachable = stage_names - reachable
            if unreachable:
                raise ValueError(f"Unreachable stages (islands): {unreachable}")
        finally:
            db_gen.close()

    @staticmethod
    def create_application(req: ApplicationCreateRequest) -> Dict:
        ApplicationService.validate(req)
        
        db_gen = get_db()
        db = next(db_gen)
        try:
            app_create = ApplicationCreate(
                name=req.name,
                description=req.description,
                input_type=req.input_type,
                stages=req.stages,
                edges=req.edges,
                entry_stage=req.entry_stage,
                exit_stages=req.exit_stages
            )
            return ApplicationService._create_application_db(db, app_create)
        finally:
            db_gen.close()

    @staticmethod
    def get_application(app_id: str) -> Optional[Dict]:
        db_gen = get_db()
        db = next(db_gen)
        try:
            app = ApplicationService._get_application_db(db, app_id)
            if app:
                return ApplicationService._get_application_dict_db(db, app.name)
            return None
        finally:
            db_gen.close()

    @staticmethod
    def get_application_by_name(name: str) -> Optional[Dict]:
        db_gen = get_db()
        db = next(db_gen)
        try:
            return ApplicationService._get_application_dict_db(db, name)
        finally:
            db_gen.close()

    @staticmethod
    def list_applications() -> Dict:
        db_gen = get_db()
        db = next(db_gen)
        try:
            apps = ApplicationService._list_applications_db(db)
            result = {}
            for name, app in apps.items():
                result[name] = ApplicationService._get_application_dict_db(db, name)
            return result
        finally:
            db_gen.close()
    
    # ========== 外部传入 DB 的接口（供 /db/v1/ 使用） ==========
    @staticmethod
    def _validate_db(db: Session, req: ApplicationCreate):
        stage_names = set(req.stages)
        for stage_name in stage_names:
            stage = db.query(Stage).filter(Stage.name == stage_name).first()
            if not stage:
                raise ValueError(f"Stage not found: {stage_name}")
        
        if req.entry_stage not in stage_names:
            raise ValueError(f"Entry stage '{req.entry_stage}' not in stages")
        
        for exit_stage in req.exit_stages:
            if exit_stage not in stage_names:
                raise ValueError(f"Exit stage '{exit_stage}' not in stages")
        
        for edge in req.edges:
            if edge.from_stage not in stage_names:
                raise ValueError(f"Edge from_stage '{edge.from_stage}' not in stages")
            if edge.to_stage not in stage_names:
                raise ValueError(f"Edge to_stage '{edge.to_stage}' not in stages")
        
        graph = defaultdict(list)
        for edge in req.edges:
            graph[edge.from_stage].append(edge.to_stage)
        
        visited = set()
        rec_stack = set()
        
        def has_cycle(node: str) -> bool:
            if node in rec_stack:
                return True
            if node in visited:
                return False
            visited.add(node)
            rec_stack.add(node)
            for neighbor in graph.get(node, []):
                if has_cycle(neighbor):
                    return True
            rec_stack.remove(node)
            return False
        
        if has_cycle(req.entry_stage):
            raise ValueError("Cycle detected, application graph must be DAG")
        
        reachable = set()
        queue = deque([req.entry_stage])
        while queue:
            node = queue.popleft()
            if node in reachable:
                continue
            reachable.add(node)
            for neighbor in graph.get(node, []):
                queue.append(neighbor)
        
        unreachable = stage_names - reachable
        if unreachable:
            raise ValueError(f"Unreachable stages (islands): {unreachable}")
    
    @staticmethod
    def _create_application_db(db: Session, req: ApplicationCreate):
        ApplicationService._validate_db(db, req)
        
        existing = db.query(Application).filter(Application.name == req.name).first()
        if existing:
            raise ValueError(f"Application already exists: {req.name}")
        
        app_id = str(uuid.uuid4())
        
        app = Application(
            app_id=app_id,
            name=req.name,
            description=req.description,
            input_type=req.input_type
        )
        db.add(app)
        db.flush()
        
        for idx, stage_name in enumerate(req.stages):
            app_stage = ApplicationStage(
                app_id=app_id,
                stage_name=stage_name,
                order_index=idx
            )
            db.add(app_stage)
        
        for edge in req.edges:
            app_edge = ApplicationEdge(
                app_id=app_id,
                from_stage=edge.from_stage,
                to_stage=edge.to_stage,
                condition=edge.condition,
                weight=edge.weight,
                is_split_point=edge.is_split_point
            )
            db.add(app_edge)
        
        app_entry = ApplicationEntry(
            app_id=app_id,
            stage_name=req.entry_stage
        )
        db.add(app_entry)
        
        for exit_stage in req.exit_stages:
            app_exit = ApplicationExit(
                app_id=app_id,
                stage_name=exit_stage
            )
            db.add(app_exit)
        
        db.commit()
        db.refresh(app)
        
        return {
            "app_id": app.app_id,
            "message": "Application registered successfully"
        }
    
    @staticmethod
    def _get_application_db(db: Session, app_id: str) -> Optional[Application]:
        return db.query(Application).filter(Application.app_id == app_id).first()
    
    @staticmethod
    def _get_application_by_name_db(db: Session, name: str) -> Optional[Application]:
        return db.query(Application).filter(Application.name == name).first()
    
    @staticmethod
    def _list_applications_db(db: Session) -> Dict:
        apps = db.query(Application).all()
        return {app.name: app for app in apps}
    
    @staticmethod
    def _get_application_dict_db(db: Session, app_name: str) -> Optional[Dict]:
        app = ApplicationService._get_application_by_name_db(db, app_name)
        if not app:
            return None
        
        app_stages = db.query(ApplicationStage).filter(ApplicationStage.app_id == app.app_id).order_by(ApplicationStage.order_index).all()
        app_edges = db.query(ApplicationEdge).filter(ApplicationEdge.app_id == app.app_id).all()
        app_entries = db.query(ApplicationEntry).filter(ApplicationEntry.app_id == app.app_id).all()
        app_exits = db.query(ApplicationExit).filter(ApplicationExit.app_id == app.app_id).all()
        
        return {
            "app_id": app.app_id,
            "name": app.name,
            "description": app.description,
            "input_type": app.input_type,
            "stages": [app_stage.stage_name for app_stage in app_stages],
            "edges": [
                {"from_stage": ae.from_stage, "to_stage": ae.to_stage}
                for ae in app_edges
            ],
            "entry_stage": app_entries[0].stage_name if app_entries else None,
            "exit_stages": [ae.stage_name for ae in app_exits]
        }
