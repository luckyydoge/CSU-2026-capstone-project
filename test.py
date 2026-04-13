import ray
import time
import uuid
from ray.util.metrics import Gauge
import monitor


runtime_env = {
    "py_modules" : [
        'monitor',
    ],
    # "working_dir": ".",  # 将当前目录上传到集群
    # "excludes": [".venv", "__pycache__", ".git"]  # 排除不需要的目录
}

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

# 2. 定义一个 Actor 类
@ray.remote
@monitor.proxy_class.monitor(submission_id="test")
class EdgeComputeActor:
    def __init__(self, actor_id=None):
        """初始化 Actor，生成唯一 ID"""
        self.actor_id = actor_id if actor_id else str(uuid.uuid4())[:8]
        self.task_count = 0
        print(f"🎬 Actor {self.actor_id} 已启动")
        tags = {"actor_id": self.actor_id}

        self.test = Gauge(
            "actor_test",
            description="test",
            tag_keys=("actor_id",)
        )
        self.test.set_default_tags(tags)
        self.test.set(1)
    
    def get_actor_id(self):
        """返回当前 Actor 的 ID"""
        return self.actor_id
    
    def compute(self, task_id):
        """执行计算任务"""
        self.task_count += 1
        print(f"[Actor {self.actor_id}] 任务 {task_id} 开始执行 (第 {self.task_count} 个任务)...")
        start_time = time.time()
        
        # 模拟耗时工作
        time.sleep(30)
        
        end_time = time.time()
        return {
            'actor_id': self.actor_id,
            'task_id': task_id,
            'task_count': self.task_count,
            'duration': f"{end_time - start_time:.2f}s"
        }
    
    def get_status(self):
        """获取 Actor 的状态"""
        return {
            'actor_id': self.actor_id,
            'task_count': self.task_count
        }

# 3. 创建多个 Actor 实例（每个有自己的 ID）
print("\n🚀 创建 3 个 Actor 实例...")

actors = [EdgeComputeActor.remote() for _ in range(3)]


# 获取每个 Actor 的 ID
actor_ids = ray.get([actor.get_actor_id.remote() for actor in actors])
for i, actor_id in enumerate(actor_ids):
    print(f"   Actor {i+1}: ID = {actor_id}")

# 4. 向 Actor 提交任务（均匀分配 10 个任务）
print("\n📤 向 Actor 提交 10 个任务...")
result_refs = []
for i in range(10):
    # 轮询分配任务到不同的 Actor
    actor = actors[i % len(actors)]
    result_refs.append(actor.compute.remote(i))

# 5. 等待结果并打印
print("⏳ 等待任务完成...\n")
results = ray.get(result_refs)
for res in results:
    print(f"✅ 任务完成: Actor {res['actor_id']} | 任务 {res['task_id']} | "
          f"已处理 {res['task_count']} 个任务 | 耗时 {res['duration']}")

# 6. 查看最终状态
print("\n📊 最终状态:")
final_status = ray.get([actor.get_status.remote() for actor in actors])
for status in final_status:
    print(f"   Actor {status['actor_id']} 总共处理了 {status['task_count']} 个任务")

ray.shutdown()
print("\n🏁 程序结束")

