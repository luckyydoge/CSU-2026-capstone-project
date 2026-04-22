import time
import uuid
from datetime import datetime
from typing import Dict, Any, List, Callable
from models.task import TaskStatus
from models.trace import StepRecord, ExecutionTrace
from storage.memory_store import STAGE_DB, APPLICATION_DB, STRATEGY_DB, DEPLOYMENT_DB
from orchestrator.strategy_loader import load_strategy   # 或直接内嵌上面函数

class SimpleExecutor:
    """模拟执行器：顺序执行 DAG 中的阶段，动态加载策略"""
    
    @staticmethod
    def _get_next_stages(app_dict: Dict, current_stage: str) -> List[str]:
        """从应用边中获取当前阶段可能的下一阶段列表"""
        edges = app_dict.get("edges", [])
        next_stages = []
        for edge in edges:
            if edge["from_stage"] == current_stage:
                next_stages.append(edge["to_stage"])
        return next_stages
    
    @staticmethod
    def _simulate_stage_execution(stage_name: str, input_data: Any) -> Any:
        """测试模式：打印阶段名，返回模拟结果"""
        time.sleep(1)
        # 模拟执行时间
        print(f"[EXECUTE] Stage: {stage_name}, input: {input_data}")
        # 模拟返回结果，可以是原输入加上阶段名
        return f"result_of_{stage_name}"
    
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
        
        # 动态加载策略
        try:
            strategy_func = load_strategy(strategy_name)
        except Exception as e:
            raise ValueError(f"Failed to load strategy '{strategy_name}': {e}")
        
        # 初始化追踪
        trace = ExecutionTrace(task_id=task_id)
        step_index = 0
        current_stage = app_dict["entry_stage"]
        current_input = input_data
        final_output = None
        
        # 执行循环
        while True:
            possible_next = SimpleExecutor._get_next_stages(app_dict, current_stage)
            # 构建上下文
            context = {
                "current_stage": current_stage,
                "input": current_input,
                "possible_next_stages": possible_next,
                "execution_history": [s.dict() for s in trace.execution_path],
                # 可扩展 node_info, deployment_configs 等
            }
            decision = strategy_func(context)
            if decision.get("should_terminate") or current_stage in app_dict.get("exit_stages", []):
                final_output = current_input
                break
            
            next_stage = decision.get("next_stage")
            if not next_stage:
                raise ValueError("Strategy returned no next_stage and not terminated")
            
            # 获取目标层级（策略可能指定）
            target_tier = decision.get("target_tier", "end")
            
            # 模拟阶段执行
            start = datetime.utcnow()
            start_perf = time.perf_counter()
            try:
                output_data = SimpleExecutor._simulate_stage_execution(next_stage, current_input)
                end_perf = time.perf_counter()
                end = datetime.utcnow()
                exec_time_ms = (end_perf - start_perf) * 1000
                
                # 模拟节点 ID（根据层级生成）
                node_id = f"sim_{target_tier}_{uuid.uuid4().hex[:4]}"
                
                step = StepRecord(
                    step_index=step_index,
                    stage_name=next_stage,
                    node_id=node_id,
                    node_tier=target_tier,
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