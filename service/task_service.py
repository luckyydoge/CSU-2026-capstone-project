import uuid
import json
from datetime import datetime
from typing import Dict, Optional, List, Any
from app.schemas import TaskCreate
from app.models import Task, Application, Strategy, ExecutionTrace, TaskStatus
from sqlalchemy.orm import Session


class TaskService:
    # ========== 外部传入 DB 的接口 ==========
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
            runtime_config=req.runtime_config,
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
                        error_msg: Optional[str] = None,
                        _commit: bool = True):
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
        if _commit:
            db.commit()
        return trace
    
    @staticmethod
    def _get_execution_traces_db(db: Session, task_id: str) -> List[ExecutionTrace]:
        return db.query(ExecutionTrace).filter(ExecutionTrace.task_id == task_id).order_by(ExecutionTrace.step_index).all()

    @staticmethod
    def _delete_task_db(db: Session, task_id: str):
        task = db.query(Task).filter(Task.task_id == task_id).first()
        if not task:
            raise ValueError(f"Task not found: {task_id}")
        db.query(ExecutionTrace).filter(ExecutionTrace.task_id == task_id).delete()
        db.delete(task)
        db.commit()
