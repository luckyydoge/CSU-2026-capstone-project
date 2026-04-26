from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import Stage, Strategy, DeploymentConfig, Application, Task, ExecutionTrace, Service
from app.schemas import (
    StageCreate, StageRead,
    StrategyCreate, StrategyRead,
    DeploymentConfigCreate, DeploymentConfigRead,
    ApplicationCreate, ApplicationRead,
    TaskCreate, TaskRead,
    ExecutionTraceRead,
    ServiceCreate, ServiceRead, ServiceUpdate,
)
from service.stage_service import StageService
from service.strategy_service import StrategyService
from service.deployment_service import DeploymentService
from service.application_service import ApplicationService
from service.task_service import TaskService
from service.service_service import ServiceService
from orchestrator.ray_executor import RayExecutor
from models.task import TaskStatus


router = APIRouter(prefix="/db/v1", tags=["database-api"])


@router.post("/stages", response_model=StageRead, status_code=201)
def create_stage(req: StageCreate, db: Session = Depends(get_db)):
    try:
        StageService._create_stage_db(db, req)
        stage = StageService._get_stage_db(db, req.name)
        return stage
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/stages", response_model=list[StageRead])
def list_stages(db: Session = Depends(get_db)):
    stages = StageService._list_stages_db(db)
    return list(stages.values())


@router.get("/stages/{name}", response_model=StageRead)
def get_stage(name: str, db: Session = Depends(get_db)):
    stage = StageService._get_stage_db(db, name)
    if not stage:
        raise HTTPException(status_code=404, detail="Stage not found")
    return stage


@router.post("/strategies", response_model=StrategyRead, status_code=201)
def create_strategy(req: StrategyCreate, db: Session = Depends(get_db)):
    try:
        StrategyService._create_strategy_db(db, req)
        strategy = StrategyService._get_strategy_db(db, req.name)
        return strategy
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/strategies", response_model=list[StrategyRead])
def list_strategies(db: Session = Depends(get_db)):
    strategies = StrategyService._list_strategies_db(db)
    return list(strategies.values())


@router.get("/strategies/{name}", response_model=StrategyRead)
def get_strategy(name: str, db: Session = Depends(get_db)):
    strategy = StrategyService._get_strategy_db(db, name)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return strategy


@router.post("/deployments", response_model=DeploymentConfigRead, status_code=201)
def create_deployment(req: DeploymentConfigCreate, db: Session = Depends(get_db)):
    try:
        DeploymentService._create_deployment_db(db, req)
        config = DeploymentService._get_deployment_db(db, req.stage_name)
        return config
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/deployments", response_model=list[DeploymentConfigRead])
def list_deployments(db: Session = Depends(get_db)):
    configs = DeploymentService._list_deployments_db(db)
    return list(configs.values())


@router.get("/deployments/{stage_name}", response_model=DeploymentConfigRead)
def get_deployment(stage_name: str, db: Session = Depends(get_db)):
    config = DeploymentService._get_deployment_db(db, stage_name)
    if not config:
        raise HTTPException(status_code=404, detail="Deployment config not found")
    return config


@router.post("/applications", response_model=ApplicationRead, status_code=201)
def create_application(req: ApplicationCreate, db: Session = Depends(get_db)):
    try:
        ApplicationService._create_application_db(db, req)
        app = ApplicationService._get_application_by_name_db(db, req.name)
        return app
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/applications", response_model=list[ApplicationRead])
def list_applications(db: Session = Depends(get_db)):
    apps = ApplicationService._list_applications_db(db)
    return list(apps.values())


@router.get("/applications/{name}", response_model=ApplicationRead)
def get_application(name: str, db: Session = Depends(get_db)):
    app = ApplicationService._get_application_by_name_db(db, name)
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")
    return app


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
    tasks = TaskService._list_tasks_db(db)
    return list(tasks.values())


@router.get("/tasks/{task_id}", response_model=TaskRead)
def get_task(task_id: str, db: Session = Depends(get_db)):
    task = TaskService._get_task_db(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@router.get("/tasks/{task_id}/traces", response_model=list[ExecutionTraceRead])
def get_task_traces(task_id: str, db: Session = Depends(get_db)):
    traces = TaskService._get_execution_traces_db(db, task_id)
    return traces


def execute_db_task(task_id: str):
    from app.database import SessionLocal
    import json
    db = SessionLocal()
    try:
        task = TaskService._get_task_db(db, task_id)
        if not task or task.status != TaskStatus.PENDING.value:
            return
        
        TaskService._update_task_status_db(db, task_id, TaskStatus.RUNNING.value)
        
        try:
            input_data = None
            if task.input_data_uri:
                try:
                    input_data = json.loads(task.input_data_uri)
                except (json.JSONDecodeError, TypeError):
                    input_data = task.input_data_uri
            
            # 兼容旧的 file_id 直接字符串的情况
            if isinstance(input_data, str) and "-" in input_data:
                input_data = {"file_id": input_data}
            
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
                                               execute_time=step_dict.get("execute_time"),
                                               end_time=step_dict.get("end_time"),
                                               queue_time_ms=step_dict.get("queue_time_ms"),
                                               actual_exec_time_ms=step_dict.get("actual_exec_time_ms"),
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
        db.close()


@router.post("/services", response_model=ServiceRead, status_code=201)
def create_service(req: ServiceCreate, db: Session = Depends(get_db)):
    try:
        ServiceService._create_service_db(db, req)
        return ServiceService._get_service_by_name_db(db, req.name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/services", response_model=list[ServiceRead])
def list_services(db: Session = Depends(get_db)):
    return ServiceService._list_services_db(db)


@router.get("/services/{service_id}", response_model=ServiceRead)
def get_service(service_id: int, db: Session = Depends(get_db)):
    service = ServiceService._get_service_db(db, service_id)
    if not service:
        raise HTTPException(status_code=404, detail="Service not found")
    return service


@router.put("/services/{service_id}", response_model=ServiceRead)
def update_service(service_id: int, req: ServiceUpdate, db: Session = Depends(get_db)):
    try:
        service = ServiceService._update_service_db(db, service_id, req)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not service:
        raise HTTPException(status_code=404, detail="Service not found")
    return service


@router.delete("/services/{service_id}", status_code=204)
def delete_service(service_id: int, db: Session = Depends(get_db)):
    deleted = ServiceService._delete_service_db(db, service_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Service not found")
