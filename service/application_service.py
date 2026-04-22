# service/application_service.py
import uuid
from typing import Dict, List, Set
from collections import defaultdict, deque

from models.application import ApplicationCreateRequest
from storage.memory_store import APPLICATION_DB, STAGE_DB

class ApplicationService:

    @staticmethod
    def validate(req: ApplicationCreateRequest):
        # 1️⃣ 检查所有引用的阶段是否已注册
        stage_names = {s.name for s in req.stages}
        for stage_name in stage_names:
            if stage_name not in STAGE_DB:
                raise ValueError(f"阶段 '{stage_name}' 未注册，请先注册该阶段")

        # 2️⃣ 检查 entry_stage 是否存在
        if req.entry_stage not in stage_names:
            raise ValueError(f"入口阶段 '{req.entry_stage}' 不在阶段列表中")

        # 3️⃣ 检查每个 exit_stage 是否存在
        for exit_stg in req.exit_stages:
            if exit_stg not in stage_names:
                raise ValueError(f"出口阶段 '{exit_stg}' 不在阶段列表中")

        # 4️⃣ 检查边的合法性
        for edge in req.edges:
            if edge.from_stage not in stage_names:
                raise ValueError(f"边 from_stage '{edge.from_stage}' 不在阶段列表中")
            if edge.to_stage not in stage_names:
                raise ValueError(f"边 to_stage '{edge.to_stage}' 不在阶段列表中")

        # 5️⃣ 构建邻接表并检查 DAG（无环）
        graph: Dict[str, List[str]] = {name: [] for name in stage_names}
        for edge in req.edges:
            graph[edge.from_stage].append(edge.to_stage)

        # 使用 DFS 检测环
        visited = set()
        rec_stack = set()

        def has_cycle(node: str) -> bool:
            if node in rec_stack:
                return True
            if node in visited:
                return False
            visited.add(node)
            rec_stack.add(node)
            for neighbor in graph[node]:
                if has_cycle(neighbor):
                    return True
            rec_stack.remove(node)
            return False

        # 从 entry_stage 开始检测环
        if has_cycle(req.entry_stage):
            raise ValueError("检测到环，应用图必须是 DAG")

        # 6️⃣ 检查所有节点是否从入口可达（避免孤岛）
        reachable = set()
        queue = [req.entry_stage]
        while queue:
            node = queue.pop()
            if node in reachable:
                continue
            reachable.add(node)
            queue.extend(graph[node])

        unreachable = stage_names - reachable
        if unreachable:
            raise ValueError(f"存在不可达的阶段（孤岛）: {unreachable}")

        # 7️⃣ 可选：检查所有出口阶段是否确实没有出边（或者允许有出边但策略可终止，暂不强制）
        # 但为了符合 DAG 语义，可以允许出口阶段仍有出边，只要策略能决定终止即可，这里不强制。

    @staticmethod
    def create_application(req: ApplicationCreateRequest):
        ApplicationService.validate(req)

        app_id = str(uuid.uuid4())
        # 保存时转为 dict（或直接存储模型对象）
        APPLICATION_DB[app_id] = req.dict()
        APPLICATION_DB[app_id]["app_id"] = app_id  # 额外存一份 id

        return {
            "app_id": app_id,
            "message": "Application registered successfully"
        }

    @staticmethod
    def get_application(app_id: str):
        return APPLICATION_DB.get(app_id)

    @staticmethod
    def list_applications():
        return APPLICATION_DB