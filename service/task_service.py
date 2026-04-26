import uuid
import json
from datetime import datetime
from typing import Dict, Optional, List, Any
from models.task import TaskCreateRequest, TaskStatus
from app.database import SessionLocal
from app.schemas import TaskCreate
from app.models import Task, Application, Strategy, ExecutionTrace
from orchestrator.ray_executor import RayExecutor
from sqlalchemy.orm import Session


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class TaskService:
    # ========== 内部管理 DB 的接口（供 /api/v1/ 使用） ==========
    @staticmethod
    def validate(req: TaskCreateRequest):
        db_gen = get_db()
        db = next(db_gen)
        try:
            app = db.query(Application).filter(Application.name == req.application_name).first()
            if not app:
                raise ValueError(f"Application not found: {req.application_name}")
            strategy = db.query(Strategy).filter(Strategy.name == req.strategy_name).first()
            if not strategy:
                raise ValueError(f"Strategy not found: {req.strategy_name}")
        finally:
            db_gen.close()

    @staticmethod
    def create_task(req: TaskCreateRequest) -> Dict:
        TaskService.validate(req)
        
        db_gen = get_db()
        db = next(db_gen)
        try:
            task_create = TaskCreate(
                app_name=req.application_name,
                strategy_name=req.strategy_name,
                input_data_uri=json.dumps(req.input_data) if req.input_data else None
            )
            return TaskService._create_task_db(db, task_create, json.dumps(req.input_data) if req.input_data else None)
        finally:
            db_gen.close()

    @staticmethod
    def get_task(task_id: str) -> Optional[Dict]:
        db_gen = get_db()
        db = next(db_gen)
        try:
            task = TaskService._get_task_db(db, task_id)
            if task:
                final_output = None
                if task.final_output_uri:
                    try:
                        final_output = json.loads(task.final_output_uri)
                    except (json.JSONDecodeError, TypeError):
                        final_output = task.final_output_uri
                return {
                    "task_id": task.task_id,
                    "application_name": task.app_name,
                    "strategy_name": task.strategy_name,
                    "status": task.status,
                    "created_at": task.created_at,
                    "completed_at": task.completed_at,
                    "final_output": final_output
                }
            return None
        finally:
            db_gen.close()

    @staticmethod
    def list_tasks() -> Dict:
        db_gen = get_db()
        db = next(db_gen)
        try:
            tasks = TaskService._list_tasks_db(db)
            result = {}
            for task_id, task in tasks.items():
                final_output = None
                if task.final_output_uri:
                    try:
                        final_output = json.loads(task.final_output_uri)
                    except (json.JSONDecodeError, TypeError):
                        final_output = task.final_output_uri
                result[task_id] = {
                    "task_id": task.task_id,
                    "application_name": task.app_name,
                    "strategy_name": task.strategy_name,
                    "status": task.status,
                    "created_at": task.created_at,
                    "completed_at": task.completed_at,
                    "final_output": final_output
                }
            return result
        finally:
            db_gen.close()
    
    @staticmethod
    def get_trace(task_id: str) -> Optional[Dict]:
        db_gen = get_db()
        db = next(db_gen)
        try:
            traces = TaskService._get_execution_traces_db(db, task_id)
            if traces:
                execution_path = []
                for trace in traces:
                    step_dict = {
                        "step_index": trace.step_index,
                        "stage_name": trace.stage_name,
                        "node_id": trace.node_id,
                        "node_tier": trace.node_tier,
                        "start_time": trace.start_time,
                        "end_time": trace.end_time,
                        "execution_time_ms": trace.execution_time_ms,
                        "transfer_time_ms": trace.transfer_time_ms,
                        "input_size_bytes": trace.input_size_bytes,
                        "output_size_bytes": trace.output_size_bytes,
                        "cpu_percent": trace.cpu_percent,
                        "memory_mb": trace.memory_mb,
                        "error_msg": trace.error_msg
                    }
                    execution_path.append(step_dict)
                return {
                    "task_id": task_id,
                    "execution_path": execution_path,
                    "error_logs": []
                }
            return None
        finally:
            db_gen.close()
    
    @staticmethod
    def execute_task(task_id: str):
        db_gen = get_db()
        db = next(db_gen)
        try:
            task = TaskService._get_task_db(db, task_id)
            if not task or task.status != TaskStatus.PENDING.value:
                return

            TaskService._update_task_status_db(db, task_id, TaskStatus.RUNNING.value)

            try:
                print(f"[execute_task] 任务 {task_id} 开始执行!")
                print(f"[execute_task] input_data_uri: {repr(task.input_data_uri)}")
                
                input_data = None
                if task.input_data_uri:
                    try:
                        input_data = json.loads(task.input_data_uri)
                        print(f"[execute_task] json.loads 成功! type={type(input_data)}, value={input_data}")
                    except (json.JSONDecodeError, TypeError) as e:
                        print(f"[execute_task] json.loads 失败: {e}, 直接使用字符串")
                        input_data = task.input_data_uri
                
                # 兼容旧的 file_id 直接字符串的情况
                if isinstance(input_data, str) and "-" in input_data:
                    print(f"[execute_task] 检测到旧格式 file_id 字符串, 转换为 dict")
                    input_data = {"file_id": input_data}
                
                print(f"[execute_task] 最终 input_data: type={type(input_data)}, value={input_data}")

                result = RayExecutor.execute(
                    task_id=task_id,
                    app_name=task.app_name,
                    strategy_name=task.strategy_name,
                    input_data=input_data
                )

                trace = result["trace"]
                execution_path = trace.get("execution_path", [])
                for step_dict in execution_path:
                    TaskService._add_trace_record_db(db, task_id, 
                                                    step_dict.get("step_index"), 
                                                    step_dict.get("stage_name"),
                                                    node_id=step_dict.get("node_id"), 
                                                    node_tier=step_dict.get("node_tier"),
                                                    start_time=step_dict.get("start_time"), 
                                                    end_time=step_dict.get("end_time"),
                                                    execution_time_ms=step_dict.get("execution_time_ms"),
                                                    transfer_time_ms=step_dict.get("transfer_time_ms"),
                                                    input_size_bytes=step_dict.get("input_size_bytes"),
                                                    output_size_bytes=step_dict.get("output_size_bytes"),
                                                    cpu_percent=step_dict.get("cpu_percent"),
                                                    memory_mb=step_dict.get("memory_mb"))

                TaskService._update_task_status_db(db, task_id, result["status"],
                                                result["final_output"])

            except Exception as e:
                import traceback
                error_msg = f"{str(e)}\n{traceback.format_exc()}"
                TaskService._add_trace_record_db(db, task_id, 0, "system",
                                               error_msg=error_msg)
                TaskService._update_task_status_db(db, task_id, TaskStatus.FAILED.value)
        finally:
            db_gen.close()
    
    # ========== 外部传入 DB 的接口（供 /db/v1/ 使用） ==========
    @staticmethod
    def _create_task_db(db: Session, req: TaskCreate, input_data_uri: Optional[str] = None):
        app = db.query(Application).filter(Application.name == req.app_name).first()
        if not app:
            raise ValueError(f"Application not found: {req.app_name}")
        
        strategy = db.query(Strategy).filter(Strategy.name == req.strategy_name).first()
        if not strategy:
            raise ValueError(f"Strategy not found: {req.strategy_name}")
        
        task_id = str(uuid.uuid4())
        
        task = Task(
            task_id=task_id,
            app_name=req.app_name,
            strategy_name=req.strategy_name,
            input_data_uri=input_data_uri or req.input_data_uri,
            status=TaskStatus.PENDING.value,
            created_at=datetime.now()
        )
        
        db.add(task)
        db.commit()
        db.refresh(task)
        
        return {
            "task_id": task.task_id,
            "message": "Task created successfully"
        }
    
    @staticmethod
    def _get_task_db(db: Session, task_id: str) -> Optional[Task]:
        return db.query(Task).filter(Task.task_id == task_id).first()
    
    @staticmethod
    def _list_tasks_db(db: Session) -> Dict:
        tasks = db.query(Task).all()
        return {task.task_id: task for task in tasks}
    
    @staticmethod
    def _update_task_status_db(db: Session, task_id: str, status: str, final_output: Optional[Any] = None):
        task = TaskService._get_task_db(db, task_id)
        if not task:
            raise ValueError(f"Task not found: {task_id}")
        
        task.status = status
        if final_output is not None:
            task.final_output_uri = json.dumps(final_output)
        if status == TaskStatus.COMPLETED.value or status == TaskStatus.FAILED.value:
            task.completed_at = datetime.now()
        
        db.commit()
        return task
    
    @staticmethod
    def _add_trace_record_db(db: Session, task_id: str, step_index: int, stage_name: str,
                        node_id: Optional[str] = None, node_tier: Optional[str] = None,
                        start_time: Optional[datetime] = None, end_time: Optional[datetime] = None,
                        execution_time_ms: Optional[float] = None, transfer_time_ms: Optional[float] = None,
                        input_size_bytes: Optional[int] = None, output_size_bytes: Optional[int] = None,
                        cpu_percent: Optional[float] = None, memory_mb: Optional[int] = None,
                        error_msg: Optional[str] = None):
        trace = ExecutionTrace(
            task_id=task_id,
            step_index=step_index,
            stage_name=stage_name,
            node_id=node_id,
            node_tier=node_tier,
            start_time=start_time,
            end_time=end_time,
            execution_time_ms=execution_time_ms,
            transfer_time_ms=transfer_time_ms,
            input_size_bytes=input_size_bytes,
            output_size_bytes=output_size_bytes,
            cpu_percent=cpu_percent,
            memory_mb=memory_mb,
            error_msg=error_msg
        )
        db.add(trace)
        db.commit()
        return trace
    
    @staticmethod
    def _get_execution_traces_db(db: Session, task_id: str) -> List[ExecutionTrace]:
        return db.query(ExecutionTrace).filter(ExecutionTrace.task_id == task_id).order_by(ExecutionTrace.step_index).all()
