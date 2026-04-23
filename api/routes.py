# api/routes.py
import os
import shutil
from fastapi import UploadFile, File, HTTPException

from fastapi import APIRouter, HTTPException, status
from datetime import datetime
from typing import Dict, List

# 模型导入
from models.stage import StageCreateRequest
from models.application import ApplicationCreateRequest
from models.deployment import DeploymentConfigCreateRequest
from models.strategy import StrategyCreateRequest
from models.task import TaskCreateRequest   # 添加这一行

# Service 导入
from service.stage_service import StageService
from service.application_service import ApplicationService
from service.deployment_service import DeploymentService
from service.strategy_service import StrategyService
from service.task_service import TaskService

STAGED_CODE_DIR = "staged_code"
router = APIRouter(prefix="/api/v1", tags=["end_edge_cloud"])

# ==================== 阶段 ====================
@router.post("/stages", status_code=status.HTTP_201_CREATED)
async def register_stage(request: StageCreateRequest):
    try:
        result = StageService.create_stage(request)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/stages")
async def list_stages():
    return StageService.list_stages()

@router.get("/stages/{name}")
async def get_stage(name: str):
    stage = StageService.get_stage(name)
    if not stage:
        raise HTTPException(status_code=404, detail="Stage not found")
    return stage
    
@router.post("/stages/upload")
async def upload_stage_code(file: UploadFile = File(...)):
    """
    上传 Python 文件，用于自定义阶段。
    文件名（不含 .py）将作为模块名，handler 格式为 "模块名:函数名"
    """
    if not file.filename.endswith(".py"):
        raise HTTPException(status_code=400, detail="Only .py files are allowed")
    
    # 安全处理文件名（防止路径遍历）
    safe_filename = os.path.basename(file.filename)
    file_path = os.path.join(STAGED_CODE_DIR, safe_filename)
    
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    module_name = safe_filename[:-3]  # 去掉 .py
    return {
        "filename": safe_filename,
        "module_name": module_name,
        "message": "Upload successful. Use handler format: f'{module_name}:function_name'"
    }
# ==================== 应用 ====================
@router.post("/applications", status_code=status.HTTP_201_CREATED)
async def register_application(request: ApplicationCreateRequest):
    try:
        result = ApplicationService.create_application(request)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/applications")
async def list_applications():
    return ApplicationService.list_applications()

@router.get("/applications/{app_id}")
async def get_application(app_id: str):
    app = ApplicationService.get_application(app_id)
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")
    return app

# ==================== 部署配置 ====================
@router.post("/deployments", status_code=status.HTTP_201_CREATED)
async def create_deployment(request: DeploymentConfigCreateRequest):
    try:
        result = DeploymentService.create_deployment(request)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/deployments")
async def list_deployments():
    return DeploymentService.list_deployments()

@router.get("/deployments/{stage_name}")
async def get_deployment(stage_name: str):
    deployment = DeploymentService.get_deployment(stage_name)
    if not deployment:
        raise HTTPException(status_code=404, detail="Deployment config not found")
    return deployment

@router.put("/deployments/{stage_name}")
async def update_deployment(stage_name: str, request: DeploymentConfigCreateRequest):
    try:
        result = DeploymentService.update_deployment(stage_name, request)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/deployments/{stage_name}")
async def delete_deployment(stage_name: str):
    try:
        result = DeploymentService.delete_deployment(stage_name)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

# ==================== 策略 ====================
@router.post("/strategies", status_code=status.HTTP_201_CREATED)
async def create_strategy(request: StrategyCreateRequest):
    try:
        result = StrategyService.create_strategy(request)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/strategies")
async def list_strategies():
    return StrategyService.list_strategies()

@router.get("/strategies/{name}")
async def get_strategy(name: str):
    strategy = StrategyService.get_strategy(name)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return strategy

@router.put("/strategies/{name}")
async def update_strategy(name: str, request: StrategyCreateRequest):
    try:
        result = StrategyService.update_strategy(name, request)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/strategies/{name}")
async def delete_strategy(name: str):
    try:
        result = StrategyService.delete_strategy(name)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

# ==================== 任务 ====================
@router.post("/tasks", status_code=status.HTTP_202_ACCEPTED)
async def submit_task(request: TaskCreateRequest):
    try:
        result = TaskService.create_task(request)
        # 同步执行（实际可改为后台任务）
        TaskService.execute_task(result["task_id"])
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/tasks")
async def list_tasks():
    return TaskService.list_tasks()

@router.get("/tasks/{task_id}")
async def get_task(task_id: str):
    task = TaskService.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

@router.get("/results/{task_id}/trace")
async def get_trace(task_id: str):
    trace = TaskService.get_trace(task_id)
    if not trace:
        raise HTTPException(status_code=404, detail="Trace not found")
    return trace