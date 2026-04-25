# api/routes.py
from fastapi import APIRouter, HTTPException, status, UploadFile, File
from fastapi.responses import StreamingResponse
from datetime import datetime
from typing import Dict, List
import io

# 模型导入
from models.stage import StageCreateRequest
from models.application import ApplicationCreateRequest
from models.deployment import DeploymentConfigCreateRequest
from models.strategy import StrategyCreateRequest
from models.task import TaskCreateRequest

# Service 导入
from service.stage_service import StageService
from service.application_service import ApplicationService
from service.deployment_service import DeploymentService
from service.strategy_service import StrategyService
from service.task_service import TaskService
from service.stage_upload_service import StageUploadService
from service.strategy_upload_service import StrategyUploadService
from service.file_service import FileService

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

# 上传相关路由必须放在 /stages/{name} 之前，避免路由冲突
@router.post("/stages/upload", status_code=status.HTTP_201_CREATED)
async def upload_stage_code(file: UploadFile = File(...)):
    try:
        content = await file.read()
        result = StageUploadService.upload_file(file.filename, content)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/stages/upload")
async def list_uploaded_files():
    """列出所有已上传的文件"""
    try:
        result = StageUploadService.list_uploaded_files()
        # 转换时间戳为ISO格式字符串
        for file_info in result["files"]:
            file_info["upload_time"] = datetime.fromtimestamp(file_info["upload_time"]).isoformat()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list files: {str(e)}")

@router.delete("/stages/upload/{filename}")
async def delete_uploaded_file(filename: str):
    """删除已上传的文件"""
    try:
        result = StageUploadService.delete_uploaded_file(filename)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete file: {str(e)}")

@router.get("/stages/{name}")
async def get_stage(name: str):
    stage = StageService.get_stage(name)
    if not stage:
        raise HTTPException(status_code=404, detail="Stage not found")
    return stage

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

# ==================== 部署 ====================
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
# 上传相关路由必须放在 /strategies/{name} 之前，避免路由冲突
@router.post("/strategies/upload", status_code=status.HTTP_201_CREATED)
async def upload_strategy_code(file: UploadFile = File(...)):

    try:
        content = await file.read()
        result = StrategyUploadService.upload_file(file.filename, content)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/strategies/upload")
async def list_uploaded_strategy_files():
    """列出所有已上传的策略文件"""
    try:
        result = StrategyUploadService.list_uploaded_files()
        # 转换时间戳为ISO格式字符串
        for file_info in result["files"]:
            file_info["upload_time"] = datetime.fromtimestamp(file_info["upload_time"]).isoformat()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list files: {str(e)}")

@router.delete("/strategies/upload/{filename}")
async def delete_uploaded_strategy_file(filename: str):
    """删除已上传的策略文件"""
    try:
        result = StrategyUploadService.delete_uploaded_file(filename)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete file: {str(e)}")

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

# ==================== 文件管理 ====================
@router.post("/files/upload", status_code=status.HTTP_201_CREATED)
async def upload_data_file(file: UploadFile = File(...)):
    """上传数据文件（图片、视频、文档等）"""
    try:
        content = await file.read()
        result = FileService.save_file(file.filename, content)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")

@router.get("/files")
async def list_files():
    """列出所有已上传的数据文件"""
    try:
        files = FileService.list_files()
        return {"files": files, "total": len(files)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list files: {str(e)}")

@router.get("/files/{file_id}")
async def get_file_info(file_id: str):
    """获取文件信息"""
    file_info = FileService.get_file(file_id)
    if not file_info:
        raise HTTPException(status_code=404, detail="File not found")
    return file_info

@router.get("/files/{file_id}/download")
async def download_file(file_id: str):
    """下载文件"""
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
        headers={
            "Content-Disposition": f'attachment; filename="{file_info["original_filename"]}"'
        }
    )

@router.delete("/files/{file_id}")
async def delete_file(file_id: str):
    """删除文件"""
    if FileService.delete_file(file_id):
        return {"message": "File deleted successfully"}
    else:
        raise HTTPException(status_code=404, detail="File not found")

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
