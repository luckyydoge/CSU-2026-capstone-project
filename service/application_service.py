import uuid
from typing import Dict, List, Set, Optional
from collections import defaultdict, deque
from app.schemas import ApplicationCreate
from app.models import (
    Application, Stage, ApplicationStage, ApplicationEdge, 
    ApplicationEntry, ApplicationExit, DataTransform
)
from sqlalchemy.orm import Session


class ApplicationService:
    # ========== 外部传入 DB 的接口 ==========
    @staticmethod
    def _validate_db(db: Session, req: ApplicationCreate):
        stage_names = set(req.stages)
        # 查询所有阶段的类型信息
        stage_rows = db.query(Stage).filter(Stage.name.in_(stage_names)).all() if stage_names else []
        type_map = {s.name: {"input_type": s.input_type, "output_type": s.output_type} for s in stage_rows}

        for stage_name in stage_names:
            if stage_name not in type_map:
                raise ValueError(f"Stage not found: {stage_name}")

        if req.entry_stage not in stage_names:
            raise ValueError(f"Entry stage '{req.entry_stage}' not in stages")

        for exit_stage in req.exit_stages:
            if exit_stage not in stage_names:
                raise ValueError(f"Exit stage '{exit_stage}' not in stages")

        # 建立入边/出边索引
        in_edges = defaultdict(set)
        out_edges = defaultdict(set)
        for edge in req.edges:
            if edge.from_stage not in stage_names:
                raise ValueError(f"Edge from_stage '{edge.from_stage}' not in stages")
            if edge.to_stage not in stage_names:
                raise ValueError(f"Edge to_stage '{edge.to_stage}' not in stages")
            out_edges[edge.from_stage].add(edge.to_stage)
            in_edges[edge.to_stage].add(edge.from_stage)

        # 入口阶段不应有入边
        if req.entry_stage in in_edges:
            raise ValueError(f"Entry stage '{req.entry_stage}' has incoming edge(s), which is not allowed")

        # 出口阶段不应有出边
        for es in req.exit_stages:
            if es in out_edges:
                raise ValueError(f"Exit stage '{es}' has outgoing edge(s), which is not allowed")

        # 输入/输出类型连续性校验
        for edge in req.edges:
            from_type = type_map.get(edge.from_stage, {}).get("output_type")
            to_type = type_map.get(edge.to_stage, {}).get("input_type")
            if from_type and to_type and from_type != to_type:
                transform = db.query(DataTransform).filter(
                    DataTransform.input_type == from_type,
                    DataTransform.output_type == to_type,
                ).first()
                if not transform:
                    raise ValueError(
                        f"Type mismatch: '{edge.from_stage}' outputs '{from_type}' "
                        f"but '{edge.to_stage}' expects '{to_type}'. "
                        f"Register a DataTransform from '{from_type}' to '{to_type}' to fix."
                    )

        graph = defaultdict(list)
        for edge in req.edges:
            graph[edge.from_stage].append(edge.to_stage)

        # 环检测
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

        # 可达性检测（独立 BFS，不依赖环检测的 visited）
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
                {"from_stage": ae.from_stage, "to_stage": ae.to_stage,
                 "is_split_point": ae.is_split_point if hasattr(ae, 'is_split_point') else False}
                for ae in app_edges
            ],
            "entry_stage": app_entries[0].stage_name if app_entries else None,
            "exit_stages": [ae.stage_name for ae in app_exits]
        }

    @staticmethod
    def _delete_application_db(db: Session, name: str):
        from app.models import Task, ExecutionTrace
        app = db.query(Application).filter(Application.name == name).first()
        if not app:
            raise ValueError(f"Application not found: {name}")
        # 先删关联的 Task 和 ExecutionTrace
        tasks = db.query(Task).filter(Task.app_name == name).all()
        for t in tasks:
            db.query(ExecutionTrace).filter(ExecutionTrace.task_id == t.task_id).delete()
            db.delete(t)
        db.delete(app)
        db.commit()
