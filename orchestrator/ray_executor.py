from typing import Any, Dict, List, Optional
from datetime import datetime
from models.task import TaskStatus
from models.trace import ExecutionTrace, StepRecord
from storage.memory_store import APPLICATION_DB, STAGE_DB
from orchestrator.strategy_loader import load_strategy
from config import CONFIG
import ray
import time

def get_stage_function(stage_name: str):
    """动态加载阶段函数"""
    import os
    import importlib.util

    stage_info = STAGE_DB.get(stage_name)
    if not stage_info:
        raise ValueError(f"Stage '{stage_name}' not found in database")

    handler = stage_info.get("handler", f"{stage_name}:run")
    module_name, func_name = handler.split(":")

    module_path = os.path.join(CONFIG.STAGED_CODE_DIR, f"{module_name}.py")

    if not os.path.exists(module_path):
        raise FileNotFoundError(f"Stage code file not found: {module_path}")

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, func_name):
        raise AttributeError(f"Function '{func_name}' not found in module '{module_name}'")

    return getattr(module, func_name)


class RayExecutor:

    @staticmethod
    def _get_next_stages(app_dict: Dict, stage_name: str) -> List[str]:
        """获取指定阶段的下一个阶段列表"""
        edges = app_dict.get("edges", [])
        next_stages = []
        for edge in edges:
            if edge.get("from_stage") == stage_name:
                next_stages.append(edge.get("to_stage"))
        return next_stages

    @staticmethod
    def _get_deployment_config(stage_name: str) -> Dict:
        """获取阶段的部署配置"""
        from storage.memory_store import DEPLOYMENT_DB
        return DEPLOYMENT_DB.get(stage_name, {
            "allowed_tiers": ["edge"],
            "resources": {"cpu_cores": 0.5, "memory_mb": 128}
        })

    @staticmethod
    def _get_tier_resource_name(tier: str) -> str:
        """获取层级对应的Ray资源名称"""
        tier_mapping = {
            "end": "tier_end",
            "edge": "tier_edge",
            "cloud": "tier_cloud"
        }
        return tier_mapping.get(tier, "tier_edge")

    @staticmethod
    def _validate_tier(target_tier: str, allowed_tiers: List[str]) -> bool:
        """验证目标层级是否在允许的层级列表中"""
        if not allowed_tiers:
            return True
        return target_tier in allowed_tiers

    @staticmethod
    def _execute_stage(stage_name: str, stage_input: Any, target_tier: str) -> Dict:
        """执行单个阶段，返回执行结果"""
        deploy_config = RayExecutor._get_deployment_config(stage_name)

        allowed_tiers = deploy_config.get("allowed_tiers", ["end", "edge", "cloud"])
        if not RayExecutor._validate_tier(target_tier, allowed_tiers):
            if allowed_tiers:
                target_tier = allowed_tiers[0]
                print(f"⚠️ 指定层级 '{target_tier}' 不在允许列表中，使用默认层级 '{target_tier}'")
            else:
                target_tier = "edge"

        resource_reqs = deploy_config.get("resources", {})
        cpu = resource_reqs.get("cpu_cores", 0.5)
        gpu = resource_reqs.get("gpu_count", 0)
        memory_mb = resource_reqs.get("memory_mb", 128)

        tier_resource = RayExecutor._get_tier_resource_name(target_tier)

        ray_options = {
            "num_cpus": cpu,
            "resources": {
                tier_resource: 1
            }
        }

        if gpu > 0:
            ray_options["num_gpus"] = gpu

        if memory_mb > 0:
            ray_options["memory"] = memory_mb * 1024 * 1024

        raw_stage_func = get_stage_function(stage_name)
        wrapped_func = RayExecutor._wrap_stage_func(raw_stage_func)
        remote_func = ray.remote(wrapped_func).options(**ray_options)

        start = datetime.utcnow()
        start_perf = time.perf_counter()
        try:
            obj_ref = remote_func.remote(stage_input)
            output_package = ray.get(obj_ref)
            output_data = output_package["result"]
            node_info = output_package["node_info"]
            end_perf = time.perf_counter()
            end = datetime.utcnow()
            exec_time_ms = (end_perf - start_perf) * 1000

            node_id = node_info.get("worker_id") or node_info.get("hostname", "unknown")
            real_tier = node_info.get("tier", "unknown")
            if real_tier == "unknown":
                real_tier = target_tier

            return {
                "result": output_data,
                "node_id": node_id,
                "node_tier": real_tier,
                "execution_time_ms": exec_time_ms,
                "start_time": start,
                "end_time": end,
                "success": True
            }
        except Exception as e:
            return {
                "error": str(e),
                "success": False
            }

    @staticmethod
    def _wrap_stage_func(stage_func):
        """包装阶段函数，添加节点信息"""
        def wrapper(input_data):
            import socket

            try:
                import ray
                node_id = ray.get_runtime_context().get_actor_id()
            except:
                node_id = "unknown"

            hostname = socket.gethostname()

            tier = "unknown"
            try:
                for tier_name, resource_name in [("end", "tier_end"), ("edge", "tier_edge"), ("cloud", "tier_cloud")]:
                    available = ray.available_resources().get(resource_name, 0)
                    if available > 0:
                        tier = tier_name
                        break
            except:
                pass

            node_info = {
                "hostname": hostname,
                "node_id": str(node_id),
                "tier": tier
            }

            result = stage_func(input_data)

            return {
                "result": result,
                "node_info": node_info
            }
        return wrapper

    @staticmethod
    def _prepare_stage_input(current_input: Any, current_stage: str) -> Any:
        """准备阶段的输入数据"""
        stage_input = current_input
        if isinstance(current_input, dict) and "file_id" in current_input:
            from service.file_service import FileService
            file_id = current_input["file_id"]
            file_content = FileService.get_file_content(file_id)
            if file_content:
                stage_input = {
                    "file_content": file_content,
                    "file_id": file_id,
                    "metadata": current_input.get("metadata", {})
                }
                print(f"[文件加载] file_id={file_id}, size={len(file_content)} bytes")
            else:
                print(f"[警告] 文件不存在: file_id={file_id}")
        return stage_input

    @staticmethod
    def execute(task_id: str, app_name: str, strategy_name: str, input_data: Any) -> Dict:
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

        exit_stages = app_dict.get("exit_stages", [])

        while True:
            possible_next = RayExecutor._get_next_stages(app_dict, current_stage)
            is_exit = current_stage in exit_stages

            context = {
                "current_stage": current_stage,
                "input": current_input,
                "possible_next_stages": possible_next,
                "execution_history": [s.dict() for s in trace.execution_path],
            }

            print(f"[决策] 当前阶段={current_stage}, 可选下一步={possible_next}, is_exit={is_exit}")

            if is_exit or not possible_next:
                print(f"[执行] 运行出口阶段: {current_stage}")
                stage_input = RayExecutor._prepare_stage_input(current_input, current_stage)
                deploy_config = RayExecutor._get_deployment_config(current_stage)
                target_tier = deploy_config.get("allowed_tiers", ["edge"])[0]
                exec_result = RayExecutor._execute_stage(current_stage, stage_input, target_tier)

                if not exec_result["success"]:
                    trace.error_logs.append(f"Stage {current_stage} failed: {exec_result.get('error')}")
                    raise Exception(exec_result.get('error'))

                step = StepRecord(
                    step_index=step_index,
                    stage_name=current_stage,
                    node_id=exec_result["node_id"],
                    node_tier=exec_result["node_tier"],
                    start_time=exec_result["start_time"],
                    end_time=exec_result["end_time"],
                    execution_time_ms=exec_result["execution_time_ms"],
                    transfer_time_ms=0.0,
                )
                trace.execution_path.append(step)
                final_output = exec_result["result"]
                print(f"[完成] 阶段 '{current_stage}' 是出口阶段，执行结束")
                break

            decision = strategy_func(context)
            
            if decision.get("should_terminate"):
                final_output = current_input
                print(f"[终止] 策略要求终止")
                break

            next_stage = decision.get("next_stage")
            if not next_stage:
                if len(possible_next) == 1:
                    next_stage = possible_next[0]
                    print(f"[默认] 策略未指定，选择唯一下一步: {next_stage}")
                else:
                    raise ValueError("Strategy returned no next_stage and no unique next stage")

            if next_stage not in possible_next:
                raise ValueError(f"Strategy returned next_stage '{next_stage}' which is not in possible_next: {possible_next}")

            target_tier = decision.get("target_tier", "edge")
            print(f"[调度决策] stage={next_stage}, target_tier={target_tier}")

            print(f"[执行] 运行阶段: {current_stage} -> {next_stage}")
            stage_input = RayExecutor._prepare_stage_input(current_input, current_stage)
            exec_result = RayExecutor._execute_stage(current_stage, stage_input, target_tier)

            if not exec_result["success"]:
                trace.error_logs.append(f"Stage {current_stage} failed: {exec_result.get('error')}")
                raise Exception(exec_result.get('error'))

            step = StepRecord(
                step_index=step_index,
                stage_name=current_stage,
                node_id=exec_result["node_id"],
                node_tier=exec_result["node_tier"],
                start_time=exec_result["start_time"],
                end_time=exec_result["end_time"],
                execution_time_ms=exec_result["execution_time_ms"],
                transfer_time_ms=0.0,
            )
            trace.execution_path.append(step)
            current_input = exec_result["result"]
            current_stage = next_stage
            step_index += 1

        trace.total_latency_ms = sum(s.execution_time_ms for s in trace.execution_path)
        return {
            "final_output": final_output,
            "trace": trace.dict(),
            "status": TaskStatus.COMPLETED
        }
