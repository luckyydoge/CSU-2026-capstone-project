import uuid
from typing import Dict, Optional, List
from sqlalchemy.orm import Session
from app.schemas import ModelCreate
from app.models import Model


class ModelService:
    @staticmethod
    def _create_model_db(db: Session, req: ModelCreate):
        model_id = str(uuid.uuid4())
        model = Model(
            model_id=model_id,
            name=req.name,
            version=req.version,
            stage_name=req.stage_name,
            weight_path=req.weight_path,
            load_method=req.load_method,
            inference_config=req.inference_config,
            alternative_models=req.alternative_models,
        )
        db.add(model)
        db.commit()
        db.refresh(model)
        return model

    @staticmethod
    def _get_model_db(db: Session, model_id: str) -> Optional[Model]:
        return db.query(Model).filter(Model.model_id == model_id).first()

    @staticmethod
    def _list_models_db(db: Session) -> Dict:
        models = db.query(Model).all()
        return {m.model_id: m for m in models}

    @staticmethod
    def _update_model_db(db: Session, model_id: str, req: ModelCreate):
        model = db.query(Model).filter(Model.model_id == model_id).first()
        if not model:
            raise ValueError(f"Model not found: {model_id}")
        model.name = req.name
        model.version = req.version
        model.stage_name = req.stage_name
        model.weight_path = req.weight_path
        model.load_method = req.load_method
        model.inference_config = req.inference_config
        model.alternative_models = req.alternative_models
        db.commit()
        db.refresh(model)
        return model

    @staticmethod
    def _delete_model_db(db: Session, model_id: str):
        model = db.query(Model).filter(Model.model_id == model_id).first()
        if not model:
            raise ValueError(f"Model not found: {model_id}")
        db.delete(model)
        db.commit()
