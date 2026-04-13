import ray
from ray.util.state import list_nodes, list_workers, list_tasks

# 1. 连接 Ray Client (用于执行分布式任务)
try:
    # 确保 kubectl port-forward ... 10001:10001 正在运行
    ray.init("ray://127.0.0.1:10001")
    print("成功连接到 Ray Cluster Client")
except Exception as e:
    print(f"Client 连接失败: {e}，尝试本地模式...")
    ray.init()

# 2. 获取状态 (显式指定 Dashboard 地址)
# 确保 kubectl port-forward ... 8265:8265 正在运行
DASHBOARD_ADDRESS = "http://127.0.0.1:8265"

try:
    print("\n--- Nodes Information ---")
    nodes = list_nodes(address=DASHBOARD_ADDRESS)
    for node in nodes:
        # 在 2.54.1 中，通常使用 'network_address' 或 'node_id'
        # 我们可以用 .get() 来安全获取，或者直接打印 node 查看所有 key
        # node_ip = node.get('network_address') or node.get('ip') or "Unknown IP"
        # print(f"ID: {node['node_id']} | IP: {node_ip} | State: {node['state']}")
        print(node)

    print("\n--- Workers Information ---")
    workers = list_workers(address=DASHBOARD_ADDRESS)
    for worker in workers:
        # 同理，Worker 结构也可能略有不同
        # print(f"WorkerID: {worker['worker_id']} | NodeID: {worker['node_id']} | Status: {worker['state']}")
        print(worker)

    print("\n--- Tasks Information ---")
    tasks = list_tasks(address=DASHBOARD_ADDRESS)
    for task in tasks:
        print(task)
except Exception as e:
    # 如果还是报错，打印一行这个能看到到底有哪些字段可以用：
    # print(f"DEBUG: First node keys: {nodes[0].keys() if nodes else 'No nodes'}")
    print(f"获取状态失败: {e}")
