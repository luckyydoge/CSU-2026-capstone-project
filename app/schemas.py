from datetime import datetime
from pydantic import BaseModel


class MonitorRecordCreate(BaseModel):
    name: str
    status: str = "active"


class MonitorRecordRead(BaseModel):
    id: int
    name: str
    status: str
    created_at: datetime

    model_config = {"from_attributes": True}
