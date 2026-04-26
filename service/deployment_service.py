from typing import Dict, Optional
from models.deployment import DeploymentConfigCreateRequest
from app.database import SessionLocal
from app.schemas import DeploymentConfigCreate
from app.models import DeploymentConfig, Stage
from sqlalchemy.orm import Session


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class DeploymentService:
    # ========== 内部管理 DB 的接口（供 /api/v1/ 使用） ==========
    @staticmethod
    def validate(req: DeploymentConfigCreateRequest):
        db_gen = get_db()
        db = next(db_gen)
        try:
            stage = db.query(Stage).filter(Stage.name == req.stage_name).first()
            if not stage:
                raise ValueError(f"Stage not found: {req.stage_name}")
            if not req.allowed_tiers:
                raise ValueError("allowed_tiers required")
            if not req.resources:
                raise ValueError("resources required")
        finally:
            db_gen.close()

    @staticmethod
    def create_deployment(req: DeploymentConfigCreateRequest) -> Dict:
        DeploymentService.validate(req)
        
        db_gen = get_db()
        db = next(db_gen)
        try:
            config_create = DeploymentConfigCreate(
                stage_name=req.stage_name,
                allowed_tiers=req.allowed_tiers,
                resources=req.resources,
                replicas=req.replicas,
                node_affinity=req.node_affinity,
                proximity=req.proximity,
                description=req.description
            )
            return DeploymentService._create_deployment_db(db, config_create)
        finally:
            db_gen.close()

    @staticmethod
    def get_deployment(stage_name: str) -> Optional[Dict]:
        db_gen = get_db()
        db = next(db_gen)
        try:
            config = DeploymentService._get_deployment_db(db, stage_name)
            if config:
                return {
                    "stage_name": config.stage_name,
                    "allowed_tiers": config.allowed_tiers,
                    "resources": config.resources,
                    "replicas": config.replicas,
                    "node_affinity": config.node_affinity,
                    "proximity": config.proximity,
                    "description": config.description
                }
            return None
        finally:
            db_gen.close()

    @staticmethod
    def list_deployments() -> Dict:
        db_gen = get_db()
        db = next(db_gen)
        try:
            configs = DeploymentService._list_deployments_db(db)
            result = {}
            for name, config in configs.items():
                result[name] = {
                    "stage_name": config.stage_name,
                    "allowed_tiers": config.allowed_tiers,
                    "resources": config.resources,
                    "replicas": config.replicas,
                    "node_affinity": config.node_affinity,
                    "proximity": config.proximity,
                    "description": config.description
                }
            return result
        finally:
            db_gen.close()
    
    @staticmethod
    def update_deployment(stage_name: str, req: DeploymentConfigCreateRequest) -> Dict:
        db_gen = get_db()
        db = next(db_gen)
        try:
            config_create = DeploymentConfigCreate(
                stage_name=req.stage_name,
                allowed_tiers=req.allowed_tiers,
                resources=req.resources,
                replicas=req.replicas,
                node_affinity=req.node_affinity,
                proximity=req.proximity,
                description=req.description
            )
            return DeploymentService._update_deployment_db(db, stage_name, config_create)
        finally:
            db_gen.close()
    
    @staticmethod
    def delete_deployment(stage_name: str) -> Dict:
        db_gen = get_db()
        db = next(db_gen)
        try:
            return DeploymentService._delete_deployment_db(db, stage_name)
        finally:
            db_gen.close()
    
    # ========== 外部传入 DB 的接口（供 /db/v1/ 使用） ==========
    @staticmethod
    def _validate_db(db: Session, req: DeploymentConfigCreate):
        stage = db.query(Stage).filter(Stage.name == req.stage_name).first()
        if not stage:
            raise ValueError(f"Stage not found: {req.stage_name}")
        if not req.allowed_tiers:
            raise ValueError("allowed_tiers required")
        if not req.resources:
            raise ValueError("resources required")
    
    @staticmethod
    def _create_deployment_db(db: Session, req: DeploymentConfigCreate):
        DeploymentService._validate_db(db, req)
        
        existing = db.query(DeploymentConfig).filter(DeploymentConfig.stage_name == req.stage_name).first()
        if existing:
            raise ValueError(f"Deployment config already exists for stage: {req.stage_name}")
        
        config = DeploymentConfig(
            stage_name=req.stage_name,
            allowed_tiers=req.allowed_tiers,
            resources=req.resources,
            replicas=req.replicas,
            node_affinity=req.node_affinity,
            proximity=req.proximity,
            description=req.description
        )
        
        db.add(config)
        db.commit()
        db.refresh(config)
        
        return {
            "stage_name": config.stage_name,
            "message": "Deployment config registered successfully"
        }
    
    @staticmethod
    def _get_deployment_db(db: Session, stage_name: str) -> Optional[DeploymentConfig]:
        return db.query(DeploymentConfig).filter(DeploymentConfig.stage_name == stage_name).first()
    
    @staticmethod
    def _list_deployments_db(db: Session) -> Dict:
        configs = db.query(DeploymentConfig).all()
        return {config.stage_name: config for config in configs}
    
    @staticmethod
    def _update_deployment_db(db: Session, stage_name: str, req: DeploymentConfigCreate):
        existing = db.query(DeploymentConfig).filter(DeploymentConfig.stage_name == stage_name).first()
        if not existing:
            raise ValueError(f"Deployment config not found for stage: {stage_name}")
        
        DeploymentService._validate_db(db, req)
        
        existing.allowed_tiers = req.allowed_tiers
        existing.resources = req.resources
        existing.replicas = req.replicas
        existing.node_affinity = req.node_affinity
        existing.proximity = req.proximity
        existing.description = req.description
        
        db.commit()
        db.refresh(existing)
        
        return {
            "stage_name": existing.stage_name,
            "message": "Deployment config updated successfully"
        }
    
    @staticmethod
    def _delete_deployment_db(db: Session, stage_name: str):
        config = db.query(DeploymentConfig).filter(DeploymentConfig.stage_name == stage_name).first()
        if not config:
            raise ValueError(f"Deployment config not found for stage: {stage_name}")
        
        db.delete(config)
        db.commit()
        
        return {
            "stage_name": stage_name,
            "message": "Deployment config deleted successfully"
        }
