from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import MonitorRecord
from app.schemas import MonitorRecordCreate, MonitorRecordRead
from monitor.controller import get_controller
import monitor.utils
import subprocess
import tempfile
import os
from pathlib import Path

router = APIRouter(
    prefix="/monitor",
    tags=["监控模块"]
)

@router.post("/records", response_model=MonitorRecordRead)
def create_record(record: MonitorRecordCreate, db: Session = Depends(get_db)):
    db_record = MonitorRecord(**record.model_dump())
    db.add(db_record)
    db.commit()
    db.refresh(db_record)
    return db_record

@router.get("/records", response_model=list[MonitorRecordRead])
def list_records(db: Session = Depends(get_db)):
    return db.query(MonitorRecord).all()

@router.get("/submission_total_cpu_usage/{submission_id}")
def get_submission_total_cpu_usage(submission_id: str):
    return monitor.utils.get_submission_total_cpu_usage(submission_id)

@router.get("/submission_total_mem_usage/{submission_id}")
def get_submission_total_mem_usage(submission_id: str):
    return monitor.utils.get_submission_total_mem_usage(submission_id)

@router.get("/proxy_actor_mem_usage/{proxy_actor_id}")
def get_proxy_actor_mem_usage(proxy_actor_id: str):
    return monitor.utils.get_proxy_actor_mem_usage(proxy_actor_id)

@router.get("/proxy_actor_cpu_usage/{proxy_actor_id}")
def get_proxy_actor_cpu_usage(proxy_actor_id: str):
    return monitor.utils.get_proxy_actor_cpu_usage(proxy_actor_id)


@router.get("/latency")
def get_latency():
    ctrl = get_controller()
    if ctrl is None:
        return {"error": "controller not initialized"}
    return ctrl.latest

@router.post("/submit")
async def submit(file: UploadFile = File(...)):
    #     # 验证文件类型
    # if not file.filename.endswith('.py'):
    #     raise HTTPException(400, "只支持 .py 文件")
    
    # 创建临时文件
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.py', delete=False) as tmp_file:
        content = await file.read()
        tmp_file.write(content)
        tmp_path = tmp_file.name
    
    try:
        # 执行 Python 文件
        result = subprocess.run(
            ['python', tmp_path],
            capture_output=True,
            text=True
        )
        
        return {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "return_code": result.returncode
        }
    finally:
        # 清理临时文件
        os.unlink(tmp_path)

