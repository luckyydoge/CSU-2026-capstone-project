from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import MonitorRecord
from app.schemas import MonitorRecordCreate, MonitorRecordRead

router = APIRouter(prefix="/monitor", tags=["monitor"])


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
