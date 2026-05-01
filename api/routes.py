from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, BackgroundTasks
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from app.database import get_db
from app.schemas import (
    StageCreate, StageRead,
    StrategyCreate, StrategyRead,
    DeploymentConfigCreate, DeploymentConfigRead,
    ApplicationCreate, ApplicationRead,
    TaskCreate, TaskRead,
    ExecutionTraceRead,
    ModelCreate, ModelRead,
    ExperimentCreate, ExperimentRead, ExperimentReport,
    DataTransformCreate, DataTransformRead,
)
from app.models import Stage, Strategy, DeploymentConfig, Application, Experiment, Task, ExecutionTrace
from service.stage_service import StageService
from service.strategy_service import StrategyService
from service.deployment_service import DeploymentService
from service.application_service import ApplicationService
from service.task_service import TaskService
from service.model_service import ModelService
from service.experiment_service import ExperimentService
from service.data_transform_service import DataTransformService
from service.node_info_service import NodeInfoService
from service.stage_upload_service import StageUploadService
from service.strategy_upload_service import StrategyUploadService
from service.file_service import FileService
from orchestrator.ray_executor import RayExecutor
from app.models import TaskStatus
from datetime import datetime
from typing import Dict, List
import io

router = APIRouter(prefix="/api/v1", tags=["end_edge_cloud"])


# ==================== 阶段（upload 路由必须在 {name} 之前） ====================
@router.post("/stages", response_model=StageRead, status_code=201)
def create_stage(req: StageCreate, db: Session = Depends(get_db)):
    try:
        StageService._create_stage_db(db, req)
        return StageService._get_stage_db(db, req.name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/stages", response_model=list[StageRead])
def list_stages(db: Session = Depends(get_db)):
    return list(StageService._list_stages_db(db).values())


@router.post("/stages/upload", status_code=201)
async def upload_stage_code(file: UploadFile = File(...)):
    try:
        content = await file.read()
        return StageUploadService.upload_file(file.filename, content)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/stages/upload")
def list_uploaded_stage_files():
    try:
        result = StageUploadService.list_uploaded_files()
        for f in result["files"]:
            f["upload_time"] = datetime.fromtimestamp(f["upload_time"]).isoformat()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/stages/upload/{filename}")
def delete_uploaded_stage_file(filename: str):
    try:
        return StageUploadService.delete_uploaded_file(filename)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stages/{name}", response_model=StageRead)
def get_stage(name: str, db: Session = Depends(get_db)):
    stage = StageService._get_stage_db(db, name)
    if not stage:
        raise HTTPException(status_code=404, detail="Stage not found")
    return stage


@router.put("/stages/{name}", response_model=StageRead)
def update_stage(name: str, req: StageCreate, db: Session = Depends(get_db)):
    try:
        if req.name != name:
            raise ValueError(f"URL stage name '{name}' does not match body stage name '{req.name}'")
        StageService._update_stage_db(db, name, req)
        return StageService._get_stage_db(db, name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/stages/{name}", status_code=204)
def delete_stage(name: str, db: Session = Depends(get_db)):
    try:
        StageService._delete_stage_db(db, name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ==================== 策略（upload 路由必须在 {name} 之前） ====================
@router.post("/strategies", response_model=StrategyRead, status_code=201)
def create_strategy(req: StrategyCreate, db: Session = Depends(get_db)):
    try:
        StrategyService._create_strategy_db(db, req)
        return StrategyService._get_strategy_db(db, req.name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/strategies", response_model=list[StrategyRead])
def list_strategies(db: Session = Depends(get_db)):
    return list(StrategyService._list_strategies_db(db).values())


@router.post("/strategies/upload", status_code=201)
async def upload_strategy_code(file: UploadFile = File(...)):
    try:
        content = await file.read()
        return StrategyUploadService.upload_file(file.filename, content)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategies/upload")
def list_uploaded_strategy_files():
    try:
        result = StrategyUploadService.list_uploaded_files()
        for f in result["files"]:
            f["upload_time"] = datetime.fromtimestamp(f["upload_time"]).isoformat()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/strategies/upload/{filename}")
def delete_uploaded_strategy_file(filename: str):
    try:
        return StrategyUploadService.delete_uploaded_file(filename)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategies/{name}", response_model=StrategyRead)
def get_strategy(name: str, db: Session = Depends(get_db)):
    strategy = StrategyService._get_strategy_db(db, name)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return strategy


@router.put("/strategies/{name}", response_model=StrategyRead)
def update_strategy(name: str, req: StrategyCreate, db: Session = Depends(get_db)):
    try:
        if req.name != name:
            raise ValueError(f"URL strategy name '{name}' does not match body strategy name '{req.name}'")
        StrategyService._update_strategy_db(db, name, req)
        return StrategyService._get_strategy_db(db, name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/strategies/{name}", status_code=204)
def delete_strategy(name: str, db: Session = Depends(get_db)):
    try:
        StrategyService._delete_strategy_db(db, name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ==================== 模型 ====================
@router.post("/models", response_model=ModelRead, status_code=201)
def create_model(req: ModelCreate, db: Session = Depends(get_db)):
    try:
        return ModelService._create_model_db(db, req)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/models", response_model=list[ModelRead])
def list_models(db: Session = Depends(get_db)):
    return list(ModelService._list_models_db(db).values())


@router.get("/models/{model_id}", response_model=ModelRead)
def get_model(model_id: str, db: Session = Depends(get_db)):
    model = ModelService._get_model_db(db, model_id)
    if not model:
        raise HTTPException(status_code=404, detail="Model not found")
    return model


@router.put("/models/{model_id}", response_model=ModelRead)
def update_model(model_id: str, req: ModelCreate, db: Session = Depends(get_db)):
    try:
        return ModelService._update_model_db(db, model_id, req)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/models/{model_id}", status_code=204)
def delete_model(model_id: str, db: Session = Depends(get_db)):
    try:
        ModelService._delete_model_db(db, model_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ==================== 数据变换 ====================
@router.post("/data-transforms", status_code=201)
def create_data_transform(req: DataTransformCreate, db: Session = Depends(get_db)):
    try:
        dt = DataTransformService._create_db(db, req.name, req.input_type, req.output_type, req.handler, req.config, req.description)
        return dt
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/data-transforms", response_model=list[DataTransformRead])
def list_data_transforms(db: Session = Depends(get_db)):
    return list(DataTransformService._list_db(db).values())


@router.get("/data-transforms/{name}", response_model=DataTransformRead)
def get_data_transform(name: str, db: Session = Depends(get_db)):
    dt = DataTransformService._get_db(db, name)
    if not dt:
        raise HTTPException(status_code=404, detail="DataTransform not found")
    return dt


@router.delete("/data-transforms/{name}", status_code=204)
def delete_data_transform(name: str, db: Session = Depends(get_db)):
    try:
        DataTransformService._delete_db(db, name)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ==================== 部署配置 ====================
@router.post("/deployments", response_model=DeploymentConfigRead, status_code=201)
def create_deployment(req: DeploymentConfigCreate, db: Session = Depends(get_db)):
    try:
        DeploymentService._create_deployment_db(db, req)
        return DeploymentService._get_deployment_db(db, req.stage_name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/deployments", response_model=list[DeploymentConfigRead])
def list_deployments(db: Session = Depends(get_db)):
    return list(DeploymentService._list_deployments_db(db).values())


@router.get("/deployments/{stage_name}", response_model=DeploymentConfigRead)
def get_deployment(stage_name: str, db: Session = Depends(get_db)):
    config = DeploymentService._get_deployment_db(db, stage_name)
    if not config:
        raise HTTPException(status_code=404, detail="Deployment config not found")
    return config


@router.put("/deployments/{stage_name}", response_model=DeploymentConfigRead)
def update_deployment(stage_name: str, req: DeploymentConfigCreate, db: Session = Depends(get_db)):
    try:
        if req.stage_name != stage_name:
            raise ValueError(f"URL stage_name '{stage_name}' does not match body stage_name '{req.stage_name}'")
        DeploymentService._update_deployment_db(db, stage_name, req)
        return DeploymentService._get_deployment_db(db, stage_name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/deployments/{stage_name}", status_code=204)
def delete_deployment(stage_name: str, db: Session = Depends(get_db)):
    try:
        DeploymentService._delete_deployment_db(db, stage_name)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ==================== 应用 ====================
@router.post("/applications", response_model=ApplicationRead, status_code=201)
def create_application(req: ApplicationCreate, db: Session = Depends(get_db)):
    try:
        ApplicationService._create_application_db(db, req)
        return ApplicationService._get_application_by_name_db(db, req.name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/applications", response_model=list[ApplicationRead])
def list_applications(db: Session = Depends(get_db)):
    return list(ApplicationService._list_applications_db(db).values())


@router.get("/applications/{name}")
def get_application(name: str, db: Session = Depends(get_db)):
    result = ApplicationService._get_application_dict_db(db, name)
    if not result:
        raise HTTPException(status_code=404, detail="Application not found")
    return result


@router.delete("/applications/{name}", status_code=204)
def delete_application(name: str, db: Session = Depends(get_db)):
    try:
        ApplicationService._delete_application_db(db, name)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ==================== 实验 ====================
@router.post("/experiments", status_code=201)
def create_experiment(req: ExperimentCreate, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    try:
        exp, task_ids = ExperimentService._create_experiment_db(db, req)
        for tid in task_ids:
            background_tasks.add_task(execute_db_task, tid)
        return {"exp_id": exp.exp_id, "name": exp.name, "task_count": len(task_ids), "message": "Experiment created"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/experiments", response_model=list[ExperimentRead])
def list_experiments(db: Session = Depends(get_db)):
    exps = ExperimentService._list_experiments_db(db)
    result = []
    for e in exps.values():
        task_count = db.query(Task).filter(Task.exp_id == e.exp_id).count()
        result.append(ExperimentRead(
            exp_id=e.exp_id, name=e.name, app_name=e.app_name,
            strategy_group=e.strategy_group, input_dataset=e.input_dataset,
            rounds=e.rounds, max_retries=e.max_retries,
            output_location=e.output_location, result_method=e.result_method,
            status=e.status, task_count=task_count,
            created_at=e.created_at, completed_at=e.completed_at,
        ))
    return result


@router.get("/experiments/{exp_id}")
def get_experiment(exp_id: str, db: Session = Depends(get_db)):
    exp = ExperimentService._get_experiment_db(db, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    task_count = db.query(Task).filter(Task.exp_id == exp_id).count()
    return ExperimentRead(
        exp_id=exp.exp_id, name=exp.name, app_name=exp.app_name,
        strategy_group=exp.strategy_group, input_dataset=exp.input_dataset,
        rounds=exp.rounds, max_retries=exp.max_retries,
        output_location=exp.output_location, result_method=exp.result_method,
        status=exp.status, task_count=task_count,
        created_at=exp.created_at, completed_at=exp.completed_at,
    )


@router.get("/experiments/{exp_id}/report", response_model=ExperimentReport)
def get_experiment_report(exp_id: str, db: Session = Depends(get_db)):
    report = ExperimentService._get_experiment_report_db(db, exp_id)
    if not report:
        raise HTTPException(status_code=404, detail="Experiment not found")
    return report


@router.delete("/experiments/{exp_id}", status_code=204)
def delete_experiment(exp_id: str, db: Session = Depends(get_db)):
    try:
        ExperimentService._delete_experiment_db(db, exp_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ==================== 任务 ====================
@router.post("/tasks", status_code=202)
def create_task(req: TaskCreate, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    try:
        result = TaskService._create_task_db(db, req)
        task_id = result["task_id"]
        background_tasks.add_task(execute_db_task, task_id)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/tasks", response_model=list[TaskRead])
def list_tasks(db: Session = Depends(get_db)):
    return list(TaskService._list_tasks_db(db).values())


@router.get("/tasks/{task_id}", response_model=TaskRead)
def get_task(task_id: str, db: Session = Depends(get_db)):
    task = TaskService._get_task_db(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@router.get("/tasks/{task_id}/traces", response_model=list[ExecutionTraceRead])
def get_task_traces(task_id: str, db: Session = Depends(get_db)):
    return TaskService._get_execution_traces_db(db, task_id)


@router.delete("/tasks/{task_id}", status_code=204)
def delete_task(task_id: str, db: Session = Depends(get_db)):
    try:
        TaskService._delete_task_db(db, task_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ==================== 文件管理 ====================
@router.post("/files/upload", status_code=201)
async def upload_data_file(file: UploadFile = File(...)):
    try:
        content = await file.read()
        return FileService.save_file(file.filename, content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")


@router.get("/files")
def list_files():
    try:
        files = FileService.list_files()
        return {"files": files, "total": len(files)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/files/{file_id}")
def get_file_info(file_id: str):
    file_info = FileService.get_file(file_id)
    if not file_info:
        raise HTTPException(status_code=404, detail="File not found")
    return file_info


@router.get("/files/{file_id}/download")
def download_file(file_id: str):
    file_info = FileService.get_file(file_id)
    if not file_info:
        raise HTTPException(status_code=404, detail="File not found")
    file_path = FileService.get_file_path(file_id)
    if not file_path:
        raise HTTPException(status_code=404, detail="File not found on disk")

    def iterfile():
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                yield chunk

    return StreamingResponse(
        iterfile(),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{file_info["original_filename"]}"'}
    )


@router.delete("/files/{file_id}")
def delete_file(file_id: str):
    if FileService.delete_file(file_id):
        return {"message": "File deleted successfully"}
    else:
        raise HTTPException(status_code=404, detail="File not found")


# ==================== 节点 ====================
@router.get("/nodes")
def list_nodes(db: Session = Depends(get_db)):
    return NodeInfoService._list_nodes_db(db)


@router.get("/nodes/{node_id}")
def get_node(node_id: str, db: Session = Depends(get_db)):
    node = NodeInfoService._get_node_db(db, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node


# ==================== 后台任务执行 ====================
def execute_db_task(task_id: str):
    from app.database import SessionLocal
    import json, re, traceback
    UUID_PATTERN = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
    db = SessionLocal()
    _db_closed = False
    try:
        task = TaskService._get_task_db(db, task_id)
        if not task or task.status != TaskStatus.PENDING.value:
            return

        TaskService._update_task_status_db(db, task_id, TaskStatus.RUNNING.value)
        task_completed = False
        result = None

        try:
            input_data = None
            if task.input_data_uri:
                try:
                    input_data = json.loads(task.input_data_uri)
                except (json.JSONDecodeError, TypeError):
                    input_data = task.input_data_uri

            if isinstance(input_data, str) and UUID_PATTERN.match(input_data):
                input_data = {"file_id": input_data}

            result = RayExecutor.execute(
                task_id=task_id,
                app_name=task.app_name,
                strategy_name=task.strategy_name,
                input_data=input_data,
                runtime_config=task.runtime_config
            )

            trace = result["trace"]
            execution_path = trace.get("execution_path", [])
            for step_dict in execution_path:
                TaskService._add_trace_record_db(db, task_id,
                    step_dict.get("step_index"), step_dict.get("stage_name"),
                    node_id=step_dict.get("node_id"), node_tier=step_dict.get("node_tier"),
                    start_time=step_dict.get("start_time"), end_time=step_dict.get("end_time"),
                    execution_time_ms=step_dict.get("execution_time_ms"),
                    transfer_time_ms=step_dict.get("transfer_time_ms"),
                    input_size_bytes=step_dict.get("input_size_bytes"),
                    output_size_bytes=step_dict.get("output_size_bytes"),
                    cpu_percent=step_dict.get("cpu_percent"),
                    memory_mb=step_dict.get("memory_mb"),
                    _commit=False)
            db.commit()

            TaskService._update_task_status_db(db, task_id, result["status"], result["final_output"])
            task_completed = True

        except Exception as e:
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            TaskService._add_trace_record_db(db, task_id, 0, "system", error_msg=error_msg)

            should_retry = False
            if task.exp_id:
                exp = ExperimentService._get_experiment_db(db, task.exp_id)
                max_r = exp.max_retries if exp else 0
                rc = task.retry_count or 0
                if rc < max_r:
                    task.retry_count = rc + 1
                    task.status = TaskStatus.PENDING.value
                    db.commit()
                    should_retry = True

            if should_retry:
                db.close()
                _db_closed = True
                execute_db_task(task_id)
                return
            else:
                TaskService._update_task_status_db(db, task_id, TaskStatus.FAILED.value)

        # 实验状态更新（独立 try-except，不受任务结果影响）
        if task.exp_id:
            try:
                pending_count = db.query(Task).filter(
                    Task.exp_id == task.exp_id,
                    Task.status.in_([TaskStatus.PENDING.value, TaskStatus.RUNNING.value])
                ).count()
                if pending_count == 0:
                    has_failed = db.query(Task).filter(
                        Task.exp_id == task.exp_id,
                        Task.status == TaskStatus.FAILED.value
                    ).count()
                    status = "completed_with_errors" if has_failed else "completed"
                    ExperimentService._update_experiment_status_db(db, task.exp_id, status)
            except Exception:
                traceback.print_exc()

    finally:
        if not _db_closed:
            db.close()
