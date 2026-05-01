from typing import Dict, Optional
from app.schemas import DeploymentConfigCreate
from app.models import DeploymentConfig, Stage
from sqlalchemy.orm import Session


class DeploymentService:
    # ========== 外部传入 DB 的接口 ==========
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
