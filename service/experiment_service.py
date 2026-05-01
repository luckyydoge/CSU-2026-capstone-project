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
            output_location=req.output_location,
            result_method=req.result_method,
            status="pending",
        )
        db.add(exp)
        db.commit()
        db.refresh(exp)

        # 自动生成实验任务: strategies × inputs × rounds
        from app.models import TaskStatus

        task_ids = []
        for strategy in req.strategy_group:
            for input_item in req.input_dataset:
                for _ in range(req.rounds):
                    task_id = str(uuid.uuid4())
                    task = Task(
                        task_id=task_id,
                        exp_id=exp_id,
                        app_name=req.app_name,
                        strategy_name=strategy,
                        input_data_uri=json.dumps(input_item),
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

        tasks = db.query(Task).filter(Task.exp_id == exp_id).order_by(Task.created_at).all()
        total = len(tasks)
        completed = sum(1 for t in tasks if t.status == "completed")
        failed = sum(1 for t in tasks if t.status == "failed")
        pending = sum(1 for t in tasks if t.status == "pending")

        all_times = []
        strategy_data = {}
        stage_data = {}
        tier_data = {}
        error_list = []
        total_transfer = 0.0
        total_cpu = []
        total_mem = []
        task_details = []

        for t in tasks:
            sdata = strategy_data.setdefault(t.strategy_name, {"total": 0, "completed": 0, "failed": 0, "times": []})
            sdata["total"] += 1
            if t.status == "completed":
                sdata["completed"] += 1
            elif t.status == "failed":
                sdata["failed"] += 1

            traces = db.query(ExecutionTrace).filter(ExecutionTrace.task_id == t.task_id).order_by(ExecutionTrace.step_index).all()
            trace_list = []
            for trace in traces:
                if trace.execution_time_ms:
                    all_times.append(trace.execution_time_ms)
                    sdata["times"].append(trace.execution_time_ms)
                sd = stage_data.setdefault(trace.stage_name, {"count": 0, "total_time": 0.0})
                sd["count"] += 1
                if trace.execution_time_ms:
                    sd["total_time"] += trace.execution_time_ms
                if trace.transfer_time_ms:
                    total_transfer += trace.transfer_time_ms
                if trace.memory_mb:
                    total_mem.append(trace.memory_mb)
                if trace.cpu_percent:
                    total_cpu.append(trace.cpu_percent)
                td = tier_data.setdefault(trace.node_tier or "unknown", {"count": 0, "total_time": 0.0})
                td["count"] += 1
                if trace.execution_time_ms:
                    td["total_time"] += trace.execution_time_ms
                if trace.error_msg:
                    error_list.append({"task_id": t.task_id, "stage": trace.stage_name, "error": trace.error_msg})
                trace_list.append({
                    "step_index": trace.step_index,
                    "stage_name": trace.stage_name,
                    "node_id": trace.node_id,
                    "node_tier": trace.node_tier,
                    "execution_time_ms": trace.execution_time_ms,
                    "transfer_time_ms": trace.transfer_time_ms,
                    "memory_mb": trace.memory_mb,
                    "cpu_percent": trace.cpu_percent,
                    "error_msg": trace.error_msg,
                })

            task_details.append({
                "task_id": t.task_id,
                "strategy_name": t.strategy_name,
                "status": t.status,
                "input_data_uri": t.input_data_uri,
                "retry_count": t.retry_count,
                "trace": trace_list,
            })

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

        tier_breakdown = []
        for tname, tdata in tier_data.items():
            tier_breakdown.append({
                "tier": tname,
                "execution_count": tdata["count"],
                "total_time_ms": tdata["total_time"],
            })

        return {
            "exp_id": exp.exp_id,
            "name": exp.name,
            "app_name": exp.app_name,
            "status": exp.status,
            "total_tasks": total,
            "completed_tasks": completed,
            "failed_tasks": failed,
            "pending_tasks": pending,
            "avg_execution_time_ms": avg_time,
            "total_transfer_time_ms": total_transfer,
            "avg_cpu_percent": sum(total_cpu) / len(total_cpu) if total_cpu else None,
            "avg_memory_mb": sum(total_mem) / len(total_mem) if total_mem else None,
            "strategy_breakdown": strategy_breakdown,
            "stage_breakdown": stage_breakdown,
            "tier_breakdown": tier_breakdown,
            "task_details": task_details,
            "errors": error_list[:50],
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

    @staticmethod
    def _delete_experiment_db(db: Session, exp_id: str):
        from app.models import Task, ExecutionTrace
        exp = db.query(Experiment).filter(Experiment.exp_id == exp_id).first()
        if not exp:
            raise ValueError(f"Experiment not found: {exp_id}")
        tasks = db.query(Task).filter(Task.exp_id == exp_id).all()
        for t in tasks:
            db.query(ExecutionTrace).filter(ExecutionTrace.task_id == t.task_id).delete()
            db.delete(t)
        db.delete(exp)
        db.commit()
