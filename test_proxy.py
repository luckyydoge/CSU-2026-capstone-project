import ray
import time
import uuid
from ray.util.metrics import Gauge
import monitor.proxy_class



# 1. 连接到 Ray 集群
try:
    ray.init("ray://127.0.0.1:10001",
             runtime_env = {"py_modules" : ['monitor',],
             })
    print("✅ 已连接到 Ray 集群 (Ray Client)")
except Exception as e:
    print(f"无法连接到 Client 端口: {e}")
    print("尝试本地模式启动...")
    ray.init()
    print("✅ 本地模式启动成功")

def compute(task_id):

    # 模拟耗时工作
    time.sleep(30)

    end_time = time.time()

proxy_acotrs = monitor.proxy_class.ProxyFactory.create([compute for _ in range(3)], 'test')



# 4. 向 Actor 提交任务（均匀分配 10 个任务）
print("\n📤 向 Actor 提交 10 个任务...")
result_refs = []
for i in range(10):
    # 轮询分配任务到不同的 Actor
    actor = proxy_acotrs[i % len(proxy_acotrs)]
    result_refs.append(actor.execute.remote(i))

# 5. 等待结果并打印
print("⏳ 等待任务完成...\n")
results = ray.get(result_refs)



ray.shutdown()
print("\n🏁 程序结束")


