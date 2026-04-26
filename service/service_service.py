from typing import Dict, Optional
from app.schemas import ServiceCreate, ServiceUpdate
from app.models import Service
from sqlalchemy.orm import Session


class ServiceService:
    @staticmethod
    def _create_service_db(db: Session, req: ServiceCreate) -> dict:
        existing = db.query(Service).filter(Service.name == req.name).first()
        if existing:
            raise ValueError(f"Service already exists: {req.name}")

        service = Service(
            name=req.name,
            prefix=req.prefix,
            file_path=req.file_path,
            num_cpus=req.num_cpus,
            num_memory=req.num_memory,
            max_replicas=req.max_replicas,
            min_replicas=req.min_replicas,
            description=req.description,
        )
        db.add(service)
        db.commit()
        db.refresh(service)
        return {"service_id": service.id, "message": "Service created successfully"}

    @staticmethod
    def _get_service_db(db: Session, service_id: int) -> Optional[Service]:
        return db.query(Service).filter(Service.id == service_id).first()

    @staticmethod
    def _get_service_by_name_db(db: Session, name: str) -> Optional[Service]:
        return db.query(Service).filter(Service.name == name).first()

    @staticmethod
    def _list_services_db(db: Session) -> list[Service]:
        return db.query(Service).order_by(Service.id).all()

    @staticmethod
    def _update_service_db(db: Session, service_id: int, req: ServiceUpdate) -> Optional[Service]:
        service = db.query(Service).filter(Service.id == service_id).first()
        if not service:
            return None

        update_data = req.model_dump(exclude_unset=True)
        if "name" in update_data:
            existing = db.query(Service).filter(
                Service.name == update_data["name"], Service.id != service_id
            ).first()
            if existing:
                raise ValueError(f"Service name already exists: {update_data['name']}")

        for key, value in update_data.items():
            setattr(service, key, value)

        db.commit()
        db.refresh(service)
        return service

    @staticmethod
    def _delete_service_db(db: Session, service_id: int) -> bool:
        service = db.query(Service).filter(Service.id == service_id).first()
        if not service:
            return False
        db.delete(service)
        db.commit()
        return True
