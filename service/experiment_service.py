import uuid
import json
from datetime import datetime
from typing import Dict, Optional, List, Any
from sqlalchemy.orm import Session
from app.schemas import ExperimentCreate
from app.models import Experiment, Task, ExecutionTrace


class ExperimentService:
    @staticmethod
    def _create_experiment_db(db: Session, req: ExperimentCreate):
        exp_id = str(uuid.uuid4())
        exp = Experiment(
            exp_id=exp_id,
            name=req.name,
            app_name=req.app_name,
            strategy_group=req.strategy_group,
            input_dataset=req.input_dataset,
            rounds=req.rounds,
            max_retries=req.max_retries,
            status="pending",
        )
        db.add(exp)
        db.commit()
        db.refresh(exp)

        # 自动生成实验任务: strategies × inputs × rounds
        from app.models import Task as TaskModel
        from models.task import TaskStatus

        task_ids = []
        for strategy in req.strategy_group:
            for input_item in req.input_dataset:
                for _ in range(req.rounds):
                    task_id = str(uuid.uuid4())
                    task = TaskModel(
                        task_id=task_id,
                        exp_id=exp_id,
                        app_name=req.app_name,
                        strategy_name=strategy,
                        input_data_uri=json.dumps(input_item) if not isinstance(input_item, str) else input_item,
                        runtime_config=None,
                        status=TaskStatus.PENDING.value,
                        created_at=datetime.now(),
                    )
                    db.add(task)
                    task_ids.append(task_id)
        db.commit()

        return exp, task_ids

    @staticmethod
    def _get_experiment_db(db: Session, exp_id: str) -> Optional[Experiment]:
        return db.query(Experiment).filter(Experiment.exp_id == exp_id).first()

    @staticmethod
    def _list_experiments_db(db: Session) -> Dict:
        exps = db.query(Experiment).all()
        return {e.exp_id: e for e in exps}

    @staticmethod
    def _get_experiment_report_db(db: Session, exp_id: str) -> Optional[Dict]:
        exp = db.query(Experiment).filter(Experiment.exp_id == exp_id).first()
        if not exp:
            return None

        tasks = db.query(Task).filter(Task.exp_id == exp_id).all()
        total = len(tasks)
        completed = sum(1 for t in tasks if t.status == "completed")
        failed = sum(1 for t in tasks if t.status == "failed")
        pending = sum(1 for t in tasks if t.status == "pending")

        # 平均执行时间
        all_times = []
        strategy_data = {}
        stage_data = {}

        for t in tasks:
            strategy_data.setdefault(t.strategy_name, {"total": 0, "completed": 0, "failed": 0, "times": []})
            strategy_data[t.strategy_name]["total"] += 1
            if t.status == "completed":
                strategy_data[t.strategy_name]["completed"] += 1
            elif t.status == "failed":
                strategy_data[t.strategy_name]["failed"] += 1

            traces = db.query(ExecutionTrace).filter(ExecutionTrace.task_id == t.task_id).all()
            for trace in traces:
                if trace.execution_time_ms:
                    all_times.append(trace.execution_time_ms)
                    strategy_data[t.strategy_name]["times"].append(trace.execution_time_ms)
                stage_data.setdefault(trace.stage_name, {"count": 0, "total_time": 0.0})
                stage_data[trace.stage_name]["count"] += 1
                if trace.execution_time_ms:
                    stage_data[trace.stage_name]["total_time"] += trace.execution_time_ms

        avg_time = sum(all_times) / len(all_times) if all_times else None

        strategy_breakdown = []
        for sname, sdata in strategy_data.items():
            avg_s = sum(sdata["times"]) / len(sdata["times"]) if sdata["times"] else None
            success_rate = sdata["completed"] / sdata["total"] if sdata["total"] > 0 else 0
            strategy_breakdown.append({
                "strategy_name": sname,
                "total": sdata["total"],
                "completed": sdata["completed"],
                "failed": sdata["failed"],
                "success_rate": success_rate,
                "avg_execution_time_ms": avg_s,
            })

        stage_breakdown = []
        for sname, sdata in stage_data.items():
            avg_s = sdata["total_time"] / sdata["count"] if sdata["count"] else None
            stage_breakdown.append({
                "stage_name": sname,
                "execution_count": sdata["count"],
                "total_time_ms": sdata["total_time"],
                "avg_execution_time_ms": avg_s,
            })

        return {
            "exp_id": exp.exp_id,
            "name": exp.name,
            "status": exp.status,
            "total_tasks": total,
            "completed_tasks": completed,
            "failed_tasks": failed,
            "avg_execution_time_ms": avg_time,
            "strategy_breakdown": strategy_breakdown,
            "stage_breakdown": stage_breakdown,
            "created_at": exp.created_at,
            "completed_at": exp.completed_at,
        }

    @staticmethod
    def _update_experiment_status_db(db: Session, exp_id: str, status: str):
        exp = db.query(Experiment).filter(Experiment.exp_id == exp_id).first()
        if not exp:
            raise ValueError(f"Experiment not found: {exp_id}")
        exp.status = status
        if status in ("completed", "failed"):
            exp.completed_at = datetime.now()
        db.commit()
        return exp
