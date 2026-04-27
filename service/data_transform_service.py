from typing import Dict, Optional, Any
from sqlalchemy.orm import Session
from app.models import DataTransform


class DataTransformService:
    @staticmethod
    def _create_db(db: Session, name: str, input_type: str, output_type: str,
                   handler: str, config: Optional[Dict] = None,
                   description: Optional[str] = None):
        existing = db.query(DataTransform).filter(DataTransform.name == name).first()
        if existing:
            raise ValueError(f"DataTransform already exists: {name}")
        dt = DataTransform(
            name=name, input_type=input_type, output_type=output_type,
            handler=handler, config=config, description=description,
        )
        db.add(dt)
        db.commit()
        db.refresh(dt)
        return dt

    @staticmethod
    def _get_db(db: Session, name: str) -> Optional[DataTransform]:
        return db.query(DataTransform).filter(DataTransform.name == name).first()

    @staticmethod
    def _list_db(db: Session) -> Dict:
        return {dt.name: dt for dt in db.query(DataTransform).all()}

    @staticmethod
    def _delete_db(db: Session, name: str):
        dt = db.query(DataTransform).filter(DataTransform.name == name).first()
        if not dt:
            raise ValueError(f"DataTransform not found: {name}")
        db.delete(dt)
        db.commit()

    @staticmethod
    def _find_transform(db: Session, input_type: str, output_type: str) -> Optional[DataTransform]:
        return db.query(DataTransform).filter(
            DataTransform.input_type == input_type,
            DataTransform.output_type == output_type,
        ).first()

    @staticmethod
    def apply_transform(handler: str, data: Any, config: Optional[Dict] = None) -> Any:
        module_path, entry = handler.split(":", 1) if ":" in handler else handler.rsplit(".", 1)
        import importlib
        module = importlib.import_module(module_path)
        func = getattr(module, entry)
        return func(data, config or {})
