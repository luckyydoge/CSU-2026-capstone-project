from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import MonitorRecord
from app.schemas import MonitorRecordCreate, MonitorRecordRead
import monitor.utils

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
>>>>>>> Conflict 1 of 1 ends
