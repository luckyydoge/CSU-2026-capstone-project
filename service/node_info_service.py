from typing import Dict, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from app.models import NodeInfo


class NodeInfoService:
    @staticmethod
    def _register_node_db(db: Session, node_id: str, hostname: str = None,
                          tier: str = None, ip_address: str = None,
                          cpu_cores: int = None, memory_mb: int = None):
        existing = db.query(NodeInfo).filter(NodeInfo.node_id == node_id).first()
        if existing:
            existing.hostname = hostname or existing.hostname
            existing.tier = tier or existing.tier
            existing.ip_address = ip_address or existing.ip_address
            if cpu_cores is not None:
                existing.cpu_cores = cpu_cores
            if memory_mb is not None:
                existing.memory_mb = memory_mb
            existing.last_heartbeat = datetime.now()
        else:
            node = NodeInfo(
                node_id=node_id, hostname=hostname, tier=tier,
                ip_address=ip_address, cpu_cores=cpu_cores, memory_mb=memory_mb,
                last_heartbeat=datetime.now(),
            )
            db.add(node)
        db.commit()

    @staticmethod
    def _update_latency_db(db: Session, node_id: str, latency_ms: float):
        node = db.query(NodeInfo).filter(NodeInfo.node_id == node_id).first()
        if node:
            node.network_latency_ms = latency_ms
            node.last_heartbeat = datetime.now()
            db.commit()

    @staticmethod
    def _update_load_db(db: Session, node_id: str, cpu_percent: float = None, memory_percent: float = None):
        node = db.query(NodeInfo).filter(NodeInfo.node_id == node_id).first()
        if node:
            if cpu_percent is not None:
                node.current_cpu_percent = cpu_percent
            if memory_percent is not None:
                node.current_memory_percent = memory_percent
            node.last_heartbeat = datetime.now()
            db.commit()

    @staticmethod
    def _list_nodes_db(db: Session) -> List[Dict]:
        nodes = db.query(NodeInfo).all()
        return [
            {
                "node_id": n.node_id, "hostname": n.hostname, "tier": n.tier,
                "ip_address": n.ip_address, "cpu_cores": n.cpu_cores,
                "memory_mb": n.memory_mb,
                "current_cpu_percent": n.current_cpu_percent,
                "current_memory_percent": n.current_memory_percent,
                "network_latency_ms": n.network_latency_ms,
                "last_heartbeat": n.last_heartbeat,
            }
            for n in nodes
        ]

    @staticmethod
    def _get_node_db(db: Session, node_id: str) -> Optional[Dict]:
        n = db.query(NodeInfo).filter(NodeInfo.node_id == node_id).first()
        if not n:
            return None
        return {
            "node_id": n.node_id, "hostname": n.hostname, "tier": n.tier,
            "ip_address": n.ip_address, "cpu_cores": n.cpu_cores,
            "memory_mb": n.memory_mb,
            "current_cpu_percent": n.current_cpu_percent,
            "current_memory_percent": n.current_memory_percent,
            "network_latency_ms": n.network_latency_ms,
            "last_heartbeat": n.last_heartbeat,
        }

    @staticmethod
    def _select_best_node_db(db: Session, tier: str, target_node: str = None) -> Optional[str]:
        """选择指定层级中的最佳节点，返回 node_id。

        优先使用 target_node（如果它在目标层级且健康），
        否则按 CPU×0.5 + 内存×0.5 加权评分选最低负载节点。
        """
        nodes = db.query(NodeInfo).filter(NodeInfo.tier == tier).all()
        if not nodes:
            return None

        if target_node:
            match = next((n for n in nodes if n.node_id == target_node), None)
            if match and (match.current_cpu_percent or 0) < 95:
                return target_node

        healthy = [n for n in nodes if (n.current_cpu_percent or 0) < 95]
        if not healthy:
            healthy = nodes

        best = min(healthy, key=lambda n:
            (n.current_cpu_percent or 0) * 0.5 + (n.current_memory_percent or 0) * 0.5)
        return best.node_id
