import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.database import SessionLocal
from service.node_info_service import NodeInfoService

db = SessionLocal()

nodes = [
    {"node_id": "node-end-1",   "hostname": "end-device-1",   "tier": "end",   "cpu_cores": 2, "memory_mb": 2048, "cpu": 85, "mem": 90},
    {"node_id": "node-edge-1",  "hostname": "edge-server-1",  "tier": "edge",  "cpu_cores": 8, "memory_mb": 16384, "cpu": 30, "mem": 45},
    {"node_id": "node-edge-2",  "hostname": "edge-server-2",  "tier": "edge",  "cpu_cores": 8, "memory_mb": 16384, "cpu": 95, "mem": 88},
    {"node_id": "node-cloud-1", "hostname": "cloud-node-1",   "tier": "cloud", "cpu_cores": 32, "memory_mb": 65536, "cpu": 50, "mem": 60},
]

for n in nodes:
    NodeInfoService._register_node_db(db,
        node_id=n["node_id"], hostname=n["hostname"], tier=n["tier"],
        cpu_cores=n["cpu_cores"], memory_mb=n["memory_mb"])
    NodeInfoService._update_load_db(db, n["node_id"], cpu_percent=n["cpu"], memory_percent=n["mem"])

db.close()

print("✅ 4 个模拟节点已创建:")
for n in nodes:
    print(f"   {n['node_id']} ({n['tier']}) CPU={n['cpu']}% 内存={n['mem']}%")
