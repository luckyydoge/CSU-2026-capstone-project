import requests
import time

BASE_URL = "http://localhost:8000/api/v1"

# 1. 注册阶段（增加 heavy_inference）
stages = [
    {"name": "data_ingest", "handler": "test_dummy:run", "input_type": "raw", "output_type": "prepared", "description": "数据接入"},
    {"name": "preprocess", "handler": "test_dummy:run", "input_type": "prepared", "output_type": "features", "description": "预处理"},
    {"name": "light_inference", "handler": "test_dummy:run", "input_type": "features", "output_type": "label", "description": "轻量推理"},
    {"name": "heavy_inference", "handler": "test_dummy:run", "input_type": "features", "output_type": "label", "description": "重量推理"},  # 新增
    {"name": "output", "handler": "test_dummy:run", "input_type": "label", "output_type": "result", "description": "结果输出"}
]

for stage in stages:
    resp = requests.post(f"{BASE_URL}/stages", json=stage)
    print(f"Register stage {stage['name']}: {resp.status_code}")

# 2. 注册应用（带分支：preprocess -> light_inference 和 preprocess -> heavy_inference）
app_req = {
    "name": "test_app_branch",
    "description": "测试分支应用",
    "input_type": "raw",
    "stages": [
        {"name": "data_ingest", "output_type": "prepared"},
        {"name": "preprocess", "output_type": "features"},
        {"name": "light_inference", "output_type": "label"},
        {"name": "heavy_inference", "output_type": "label"},
        {"name": "output", "output_type": "result"}
    ],
    "edges": [
        {"from_stage": "data_ingest", "to_stage": "preprocess"},
        {"from_stage": "preprocess", "to_stage": "light_inference"},
        {"from_stage": "preprocess", "to_stage": "heavy_inference"},   # 分支边
        {"from_stage": "light_inference", "to_stage": "output"},
        {"from_stage": "heavy_inference", "to_stage": "output"}
    ],
    "entry_stage": "data_ingest",
    "exit_stages": ["output"]
}
resp = requests.post(f"{BASE_URL}/applications", json=app_req)
print(f"Register app: {resp.status_code}", resp.json())

# 3. 部署配置（可选，为 heavy_inference 也添加）
deployments = [
    {"stage_name": "data_ingest", "allowed_tiers": ["edge"], "resources": {"cpu_cores": 0.5, "memory_mb": 128}, "replicas": 1},
    {"stage_name": "preprocess", "allowed_tiers": ["edge"], "resources": {"cpu_cores": 0.5, "memory_mb": 256}, "replicas": 1},
    {"stage_name": "light_inference", "allowed_tiers": ["edge"], "resources": {"cpu_cores": 0.5, "memory_mb": 512}, "replicas": 1},
    {"stage_name": "heavy_inference", "allowed_tiers": ["edge"], "resources": {"cpu_cores": 1.0, "memory_mb": 1024}, "replicas": 1},
    {"stage_name": "output", "allowed_tiers": ["edge"], "resources": {"cpu_cores": 0.2, "memory_mb": 64}, "replicas": 1}
]
for dep in deployments:
    resp = requests.post(f"{BASE_URL}/deployments", json=dep)
    print(f"Deployment for {dep['stage_name']}: {resp.status_code}")

# 4. 注册策略（随机路由）
strategy_req = {
    "name": "random_router",
    "strategy_type": "routing",
    "handler": "strategies.random_routing:decide",
    "config": {},
    "description": "随机选择下一阶段"
}
resp = requests.post(f"{BASE_URL}/strategies", json=strategy_req)
print(f"Register strategy: {resp.status_code}", resp.json())

# 5. 提交任务
task_req = {
    "application_name": "test_app_branch",
    "strategy_name": "random_router",
    "input_data": "hello_world",
    "runtime_config": {}
}
resp = requests.post(f"{BASE_URL}/tasks", json=task_req)
print(f"Submit task: {resp.status_code}", resp.json())
task_id = resp.json()["task_id"]
print(f"Task ID: {task_id}")

# 等待执行完成
time.sleep(1)
task_resp = requests.get(f"{BASE_URL}/tasks/{task_id}")
print("Task result:", task_resp.json())

# 导出 trace
trace_resp = requests.get(f"{BASE_URL}/results/{task_id}/trace")
print("Trace:", trace_resp.json())