from sqlalchemy import Column, Integer, String, DateTime, func
from app.database import Base


class MonitorRecord(Base):
    __tablename__ = "monitor_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    status = Column(String, default="active")
    created_at = Column(DateTime, server_default=func.now())
