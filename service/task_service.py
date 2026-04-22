import uuid
from datetime import datetime
from typing import Dict, Optional
from models.task import TaskCreateRequest, TaskStatus
from models.trace import ExecutionTrace   # 如果之前写错了也一并修正
from storage.memory_store import TASK_DB, TRACE_DB, APPLICATION_DB, STRATEGY_DB
from orchestrator.simple_executor import SimpleExecutor
from orchestrator.ray_executor import RayExecutor

class TaskService:
    @staticmethod
    def validate(req: TaskCreateRequest):
        if not any(app.get("name") == req.application_name for app in APPLICATION_DB.values()):
            raise ValueError(f"Application '{req.application_name}' not found")
        if req.strategy_name not in STRATEGY_DB:
            raise ValueError(f"Strategy '{req.strategy_name}' not found")
    
    @staticmethod
    def create_task(req: TaskCreateRequest) -> Dict:
        TaskService.validate(req)
        task_id = str(uuid.uuid4())
        now = datetime.utcnow()
        task_data = {
            "task_id": task_id,
            "application_name": req.application_name,
            "strategy_name": req.strategy_name,
            "status": TaskStatus.PENDING,
            "created_at": now,
            "completed_at": None,
            "final_output": None,
            "input_data": req.input_data,   # 临时保存
            "runtime_config": req.runtime_config,
        }
        TASK_DB[task_id] = task_data
        return {"task_id": task_id, "status": TaskStatus.PENDING, "created_at": now}
    
    @staticmethod
    def execute_task(task_id: str):
        task = TASK_DB.get(task_id)
        if not task or task["status"] != TaskStatus.PENDING:
            return
        task["status"] = TaskStatus.RUNNING
        try:
            # 直接同步调用
            result = RayExecutor.execute(
                task_id=task_id,
                app_name=task["application_name"],
                strategy_name=task["strategy_name"],
                input_data=task["input_data"]
            )
            TRACE_DB[task_id] = result["trace"]
            task["status"] = result["status"]
            task["final_output"] = result["final_output"]
            task["trace"] = result["trace"]
            task["completed_at"] = datetime.utcnow()
        except Exception as e:
            task["status"] = TaskStatus.FAILED
            task["completed_at"] = datetime.utcnow()
            if not task.get("trace"):
                task["trace"] = {"task_id": task_id, "execution_path": [], "error_logs": [str(e)]}
            else:
                task["trace"]["error_logs"].append(str(e))
        finally:
            task.pop("input_data", None)
    
    @staticmethod
    def get_task(task_id: str) -> Optional[Dict]:
        task = TASK_DB.get(task_id)
        if task:
            return {k: v for k, v in task.items() if k not in ["input_data", "runtime_config"]}
        return None
    
    @staticmethod
    def list_tasks() -> Dict:
        return {tid: {k:v for k,v in task.items() if k not in ["input_data","runtime_config"]}
                for tid, task in TASK_DB.items()}
    
    @staticmethod
    def get_trace(task_id: str) -> Optional[Dict]:
        return TRACE_DB.get(task_id)