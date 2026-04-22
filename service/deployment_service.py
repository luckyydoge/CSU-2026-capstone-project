# service/deployment_service.py
from typing import Dict, Optional, List
from models.deployment import DeploymentConfigCreateRequest, ResourceRequirements, Tier
from storage.memory_store import STAGE_DB, DEPLOYMENT_DB

class DeploymentService:

    @staticmethod
    def _validate_tiers(tiers: List[Tier]) -> None:
        """校验逻辑层级列表是否合法"""
        valid_tiers = {"end", "edge", "cloud"}
        for t in tiers:
            if t not in valid_tiers:
                raise ValueError(f"非法逻辑层级: {t}，允许值: end, edge, cloud")
        if not tiers:
            raise ValueError("allowed_tiers 不能为空")

    @staticmethod
    def _validate_resources(res: ResourceRequirements) -> None:
        """校验资源需求"""
        if res.cpu_cores <= 0:
            raise ValueError("cpu_cores 必须大于 0")
        if res.memory_mb <= 0:
            raise ValueError("memory_mb 必须大于 0")
        if res.gpu_count < 0:
            raise ValueError("gpu_count 不能为负数")
        if res.gpu_memory_mb is not None and res.gpu_memory_mb <= 0:
            raise ValueError("gpu_memory_mb 必须大于 0（如果提供）")

    @staticmethod
    def validate(req: DeploymentConfigCreateRequest) -> None:
        """完整校验部署配置"""
        # 1. 阶段必须已注册
        if req.stage_name not in STAGE_DB:
            raise ValueError(f"阶段 '{req.stage_name}' 未注册，请先注册阶段")

        # 2. 校验 allowed_tiers
        DeploymentService._validate_tiers(req.allowed_tiers)

        # 3. 校验资源需求
        DeploymentService._validate_resources(req.resources)

        # 4. 校验副本数
        if req.replicas < 1:
            raise ValueError("replicas 必须 >= 1")

        # 5. 可选：校验 node_affinity 中的节点名称或标签格式（简单示例）
        if req.node_affinity:
            if req.node_affinity.node_names and not all(isinstance(n, str) for n in req.node_affinity.node_names):
                raise ValueError("node_names 必须是字符串列表")
            if req.node_affinity.match_labels and not isinstance(req.node_affinity.match_labels, dict):
                raise ValueError("match_labels 必须是字典")

        # 6. 可选：校验 proximity 中的目标阶段是否存在
        if req.proximity:
            if req.proximity.target_stage not in STAGE_DB:
                raise ValueError(f"邻近部署的目标阶段 '{req.proximity.target_stage}' 未注册")

    @staticmethod
    def create_deployment(req: DeploymentConfigCreateRequest) -> Dict:
        """创建部署配置（一个阶段只能有一个）"""
        DeploymentService.validate(req)

        if req.stage_name in DEPLOYMENT_DB:
            raise ValueError(f"阶段 '{req.stage_name}' 已存在部署配置，请使用更新接口")

        # 存储为字典
        DEPLOYMENT_DB[req.stage_name] = req.dict()
        return {
            "stage_name": req.stage_name,
            "message": "Deployment configuration created successfully"
        }

    @staticmethod
    def get_deployment(stage_name: str) -> Optional[Dict]:
        """根据阶段名称获取部署配置"""
        return DEPLOYMENT_DB.get(stage_name)

    @staticmethod
    def list_deployments() -> Dict:
        """列出所有部署配置（键为阶段名）"""
        return DEPLOYMENT_DB

    @staticmethod
    def update_deployment(stage_name: str, req: DeploymentConfigCreateRequest) -> Dict:
        """更新部署配置（阶段必须已存在配置）"""
        if stage_name not in DEPLOYMENT_DB:
            raise ValueError(f"阶段 '{stage_name}' 的部署配置不存在，无法更新")

        # 校验时要求阶段名称与传入的 stage_name 一致
        if req.stage_name != stage_name:
            raise ValueError("请求中的 stage_name 与路径参数不匹配")

        DeploymentService.validate(req)

        DEPLOYMENT_DB[stage_name] = req.dict()
        return {
            "stage_name": stage_name,
            "message": "Deployment configuration updated successfully"
        }

    @staticmethod
    def delete_deployment(stage_name: str) -> Dict:
        """删除部署配置"""
        if stage_name not in DEPLOYMENT_DB:
            raise ValueError(f"阶段 '{stage_name}' 的部署配置不存在")
        del DEPLOYMENT_DB[stage_name]
        return {
            "stage_name": stage_name,
            "message": "Deployment configuration deleted successfully"
        }