import ray
import time
from typing import Any
from .handlers.base import BaseTaskHandler
from .task_plan import TaskPlan
from .config import CONFIG  


class EdgeOrchestrator:
    def __init__(self, ray_address: str = None):
        # 优先使用传入参数，否则从 CONFIG 读取
        self.ray_address = ray_address or CONFIG.RAY_ADDRESS

        if not ray.is_initialized():
            print(f"📡 正在尝试连接远程 Ray 集群: {self.ray_address}...")
            try:
                ray.init(address=self.ray_address, ignore_reinit_error=True)
                print(f"✅ 成功连接至服务器集群！")
            except Exception as e:
                print(f"❌ 错误: 无法连接到远程 Ray 集群 ({self.ray_address})")
                print(f"💡 请检查: 1. 服务器 Ray 是否启动; 2. 端口转发是否生效; 3. 网络是否可达")
                raise ConnectionError(f"CRITICAL: Failed to connect to remote Ray cluster at {self.ray_address}. Local fallback disabled.")


    async def execute(self, handler: BaseTaskHandler, processed_data: Any):
        plan = handler.build_plan(processed_data)

        worker_fn = plan.worker_fn
        if worker_fn is None:
            def default_worker(chunk):
                time.sleep(0.1)
                return str(chunk)[:50]
            worker_fn = default_worker

        worker_fn_ref = ray.put(worker_fn)

        @ray.remote
        def dynamic_worker(chunk, fn_ref):
            fn = ray.get(fn_ref)
            return fn(chunk)

        futures = []
        for chunk, res in zip(plan.chunks, plan.chunk_resources):
            # 构建 Ray 资源选项
            options = {}
            if "CPU" in res:
                options["num_cpus"] = res["CPU"]
            if "GPU" in res:
                options["num_gpus"] = res["GPU"]
            # 直接将 worker_fn 转为 Ray remote 函数并提交
            remote_worker = ray.remote(worker_fn).options(**options)
            futures.append(remote_worker.remote(chunk))

        mid_results = ray.get(futures)

        if plan.aggregator_fn:
            final_result = plan.aggregator_fn(mid_results)
        else:
            final_result = mid_results

        if plan.display_formatter:
            final_result = plan.display_formatter(final_result)

        return final_result, {"chunks": len(plan.chunks)}