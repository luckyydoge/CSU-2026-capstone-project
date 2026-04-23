import requests
import time
import os
import uuid

BASE_URL = "http://localhost:8000/api/v1"

suffix = uuid.uuid4().hex[:6]
filter_stage_name = f"half_filter_{suffix}"
identity_stage_name = f"identity_{suffix}"
app_name = f"test_app_{suffix}"

print(f"Filter stage: {filter_stage_name}")
print(f"Identity stage: {identity_stage_name}")
print(f"App name: {app_name}")

# 1. 准备自定义阶段代码（half_filter）
custom_code = """
def run(data):
    if isinstance(data, list):
        return data[:len(data)//2]
    return data
"""
with open("half_filter.py", "w") as f:
    f.write(custom_code)

# 2. 上传 half_filter 代码
with open("half_filter.py", "rb") as f:
    upload_resp = requests.post(f"{BASE_URL}/stages/upload", files={"file": f})
print("Upload half_filter:", upload_resp.status_code, upload_resp.json())
module_name = upload_resp.json()["module_name"]

# 3. 注册 half_filter 阶段
stage_req = {
    "name": filter_stage_name,
    "handler": f"{module_name}:run",
    "input_type": "json",
    "output_type": "json",
    "description": "保留前一半"
}
resp = requests.post(f"{BASE_URL}/stages", json=stage_req)
print("Register filter stage:", resp.status_code)
if resp.status_code != 201:
    print("Error:", resp.text)
    exit(1)

# 4. 注册一个简单的恒等阶段（identity）
# 该阶段直接返回输入，可以使用内置的 test_dummy:run 或上传一个简单函数
# 为了不依赖外部，我们动态生成并上传 identity 代码
identity_code = """
def run(data):
    return data
"""
with open("identity.py", "w") as f:
    f.write(identity_code)
with open("identity.py", "rb") as f:
    upload_id = requests.post(f"{BASE_URL}/stages/upload", files={"file": f})
print("Upload identity:", upload_id.status_code, upload_id.json())
id_module = upload_id.json()["module_name"]
id_req = {
    "name": identity_stage_name,
    "handler": f"{id_module}:run",
    "input_type": "json",
    "output_type": "json",
    "description": "恒等函数"
}
resp = requests.post(f"{BASE_URL}/stages", json=id_req)
print("Register identity stage:", resp.status_code)
if resp.status_code != 201:
    print("Error:", resp.text)
    exit(1)

# 5. 注册应用（half_filter -> identity）
app_req = {
    "name": app_name,
    "description": "测试自定义阶段",
    "input_type": "json",
    "stages": [
        {"name": filter_stage_name, "output_type": "json"},
        {"name": identity_stage_name, "output_type": "json"}
    ],
    "edges": [
        {"from_stage": filter_stage_name, "to_stage": identity_stage_name}
    ],
    "entry_stage": filter_stage_name,
    "exit_stages": [identity_stage_name]
}
resp = requests.post(f"{BASE_URL}/applications", json=app_req)
print("Register application:", resp.status_code)
if resp.status_code != 201:
    print("Error:", resp.text)
    exit(1)
print("App registered.")

# 6. 部署配置（为两个阶段设置简单资源）
deployments = [
    {"stage_name": filter_stage_name, "allowed_tiers": ["edge"], "resources": {"cpu_cores": 0.5, "memory_mb": 128}, "replicas": 1},
    {"stage_name": identity_stage_name, "allowed_tiers": ["edge"], "resources": {"cpu_cores": 0.2, "memory_mb": 64}, "replicas": 1}
]
for dep in deployments:
    resp = requests.post(f"{BASE_URL}/deployments", json=dep)
    if resp.status_code == 409:
        # 如果已存在，尝试更新
        requests.put(f"{BASE_URL}/deployments/{dep['stage_name']}", json=dep)
    else:
        print(f"Deployment for {dep['stage_name']}: {resp.status_code}")

# 7. 策略（使用 random_router，若不存在则注册）
strategy_req = {
    "name": "random_router",
    "strategy_type": "routing",
    "handler": "strategies.random_routing:decide",
    "config": {},
    "description": "随机选择下一阶段"
}
resp = requests.post(f"{BASE_URL}/strategies", json=strategy_req)
if resp.status_code == 409:
    print("Strategy already exists.")
else:
    print("Register strategy:", resp.status_code)

# 8. 提交任务
task_req = {
    "application_name": app_name,
    "strategy_name": "random_router",
    "input_data": [1,2,3,4,5,6,7,8,9,10],
    "runtime_config": {}
}
resp = requests.post(f"{BASE_URL}/tasks", json=task_req)
print("Submit task:", resp.status_code)
if resp.status_code != 202:
    print("Error:", resp.text)
    exit(1)
task_id = resp.json()["task_id"]
print("Task ID:", task_id)

time.sleep(2)
task_resp = requests.get(f"{BASE_URL}/tasks/{task_id}")
print("Task result:", task_resp.json())

trace_resp = requests.get(f"{BASE_URL}/results/{task_id}/trace")
if trace_resp.status_code == 200:
    print("Trace:", trace_resp.json())
else:
    print("Trace not found.")

# 清理临时文件
os.remove("half_filter.py")
os.remove("identity.py")
print("Test completed.")