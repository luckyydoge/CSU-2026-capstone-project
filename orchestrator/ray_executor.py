import ray
import importlib
import uuid
import time
import socket
from datetime import datetime
from typing import Dict, Any, List
from models.task import TaskStatus
from models.trace import StepRecord, ExecutionTrace
from storage.memory_store import STAGE_DB, APPLICATION_DB, STRATEGY_DB, DEPLOYMENT_DB
from orchestrator.strategy_loader import load_strategy
from config import CONFIG

# 初始化 Ray（保持不变）
if not ray.is_initialized():
    if CONFIG.RAY_ADDRESS:
        print(f"📡 正在连接远程 Ray 集群: {CONFIG.RAY_ADDRESS}")
        try:
            ray.init(address=CONFIG.RAY_ADDRESS,
                     runtime_env={"working_dir": "."},
                     ignore_reinit_error=True)
            print("✅ 成功连接到远程 Ray 集群")
        except Exception as e:
            error_msg = f"连接远程 Ray 集群失败: {e}"
            if CONFIG.FALLBACK_LOCAL:
                print(f"⚠️ {error_msg}，回退到本地模式")
                ray.init(runtime_env={"working_dir": "."})
            else:
                raise ConnectionError(error_msg)
    else:
        print("🚀 未配置远程 Ray 地址，启动本地 Ray 集群")
        ray.init(runtime_env={"working_dir": "."})

def get_stage_function(stage_name: str):
    """根据阶段名称动态加载可执行函数"""
    from storage.memory_store import STAGE_DB
    stage_info = STAGE_DB.get(stage_name)
    if not stage_info:
        raise ValueError(f"Stage '{stage_name}' not registered")
    handler = stage_info["handler"]
    if ":" not in handler:
        raise ValueError(f"Invalid handler format: {handler}. Expected 'module:function'")
    module_name, func_name = handler.split(":", 1)
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise ImportError(f"Failed to import module '{module_name}': {e}. Ensure file is uploaded to staged_code/")
    func = getattr(module, func_name, None)
    if not callable(func):
        raise ValueError(f"Function '{func_name}' not found or not callable in module '{module_name}'")
    return func

class RayExecutor:
    @staticmethod
    def _wrap_stage_func(stage_func):
        """
        通用包装器：执行原始阶段函数，并自动附加节点信息。
        返回字典格式：{"result": 原始结果, "node_info": {...}}
        """
        def wrapped(input_data):
            # 1. 执行原始业务逻辑
            result = stage_func(input_data)
            
            # 2. 获取节点信息
            node_info = {}
            try:
                ctx = ray.get_runtime_context()
                node_info["worker_id"] = ctx.get_worker_id()
                node_info["node_id"] = ctx.get_node_id()
            except Exception:
                node_info["worker_id"] = node_info["node_id"] = "unknown"
            
            try:
                hostname = socket.gethostname()
                node_info["hostname"] = hostname
                node_info["ip"] = socket.gethostbyname(hostname)
            except Exception:
                node_info["hostname"] = node_info["ip"] = "unknown"
            
            # 尝试获取节点资源标签（需节点启动时设置 --resources）
            node_info["tier"] = "unknown"
            try:
                # 注意：ray.worker.global_worker.node 在某些版本可能不存在，使用 try
                if hasattr(ray.worker, "global_worker") and hasattr(ray.worker.global_worker, "node"):
                    resources = ray.worker.global_worker.node.resources
                    node_info["tier"] = resources.get("tier", "unknown")
            except Exception:
                pass
            
            return {"result": result, "node_info": node_info}
        return wrapped

    @staticmethod
    def _get_next_stages(app_dict: Dict, current_stage: str) -> List[str]:
        edges = app_dict.get("edges", [])
        return [edge["to_stage"] for edge in edges if edge["from_stage"] == current_stage]

    @staticmethod
    def _get_deployment_config(stage_name: str) -> Dict:
        return DEPLOYMENT_DB.get(stage_name, {})

    @staticmethod
    def execute(task_id: str, app_name: str, strategy_name: str, input_data: Any) -> Dict:
        # 获取应用定义
        app_dict = None
        for app in APPLICATION_DB.values():
            if app.get("name") == app_name:
                app_dict = app
                break
        if not app_dict:
            raise ValueError(f"Application '{app_name}' not found")

        strategy_func = load_strategy(strategy_name)

        trace = ExecutionTrace(task_id=task_id)
        step_index = 0
        current_stage = app_dict["entry_stage"]
        current_input = input_data
        final_output = None

        while True:
            possible_next = RayExecutor._get_next_stages(app_dict, current_stage)
            context = {
                "current_stage": current_stage,
                "input": current_input,
                "possible_next_stages": possible_next,
                "execution_history": [s.dict() for s in trace.execution_path],
            }
            decision = strategy_func(context)
            if decision.get("should_terminate") or current_stage in app_dict.get("exit_stages", []):
                final_output = current_input
                break

            next_stage = decision.get("next_stage")
            if not next_stage:
                raise ValueError("Strategy returned no next_stage and not terminated")

            target_tier = decision.get("target_tier", "edge")
            deploy_config = RayExecutor._get_deployment_config(next_stage)
            resource_reqs = deploy_config.get("resources", {})
            cpu = resource_reqs.get("cpu_cores", 0.5)
            gpu = resource_reqs.get("gpu_count", 0)

            ray_options = {"num_cpus": cpu}
            if gpu > 0:
                ray_options["num_gpus"] = gpu

            # 获取原始阶段函数并包装
            raw_stage_func = get_stage_function(next_stage)
            wrapped_func = RayExecutor._wrap_stage_func(raw_stage_func)
            remote_func = ray.remote(wrapped_func).options(**ray_options)

            start = datetime.utcnow()
            start_perf = time.perf_counter()
            try:
                obj_ref = remote_func.remote(current_input)
                output_package = ray.get(obj_ref)      # 包含 result 和 node_info
                output_data = output_package["result"]
                node_info = output_package["node_info"]
                end_perf = time.perf_counter()
                end = datetime.utcnow()
                exec_time_ms = (end_perf - start_perf) * 1000

                # 提取节点标识：优先使用 worker_id，否则使用 hostname
                node_id = node_info.get("worker_id") or node_info.get("hostname", "unknown")
                # 节点层级：优先使用真实的资源标签，否则回退到策略的 target_tier
                real_tier = node_info.get("tier", "unknown")
                if real_tier == "unknown":
                    real_tier = target_tier

                step = StepRecord(
                    step_index=step_index,
                    stage_name=next_stage,
                    node_id=node_id,
                    node_tier=real_tier,
                    start_time=start,
                    end_time=end,
                    execution_time_ms=exec_time_ms,
                    transfer_time_ms=0.0,
                )
                trace.execution_path.append(step)
                current_input = output_data
                current_stage = next_stage
                step_index += 1
            except Exception as e:
                trace.error_logs.append(f"Stage {next_stage} failed: {str(e)}")
                raise

        trace.total_latency_ms = sum(s.execution_time_ms for s in trace.execution_path)
        return {
            "final_output": final_output,
            "trace": trace.dict(),
            "status": TaskStatus.COMPLETED
        }