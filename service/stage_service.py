# service/stage_service.py
import uuid
from typing import Dict, List, Optional
from models.stage import StageCreateRequest
from storage.memory_store import STAGE_DB

class StageService:

    @staticmethod
    def validate(req: StageCreateRequest):
        # 1️⃣ name 必须存在
        if not req.name or not req.name.strip():
            raise ValueError("Stage name 不能为空")

        # 2️⃣ handler 必须合法（简单校验包含点或冒号）
        if "." not in req.handler and ":" not in req.handler:
            raise ValueError("handler 必须是 module.function 或 module:ClassName 格式")

        # 3️⃣ input_type / output_type 不能为空
        if not req.input_type:
            raise ValueError("input_type 不能为空")
        if not req.output_type:
            raise ValueError("output_type 不能为空")

        # 4️⃣ 可选：检查模型名称（如果提供，可校验是否存在于模型注册表，暂略）

    @staticmethod
    def create_stage(req: StageCreateRequest):
        StageService.validate(req)

        if req.name in STAGE_DB:
            raise ValueError(f"Stage 已存在: {req.name}")

        # 保存为字典（可转为 dict）
        STAGE_DB[req.name] = req.dict()

        return {
            "stage_name": req.name,
            "message": "Stage registered successfully"
        }

    @staticmethod
    def get_stage(name: str) -> Optional[Dict]:
        return STAGE_DB.get(name)

    @staticmethod
    def list_stages() -> Dict:
        return STAGE_DB