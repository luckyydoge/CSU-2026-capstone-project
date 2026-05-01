from typing import Any, Dict, List, Optional
from datetime import datetime
from app.models import TaskStatus
from app.schemas import ExecutionTraceSchema, StepRecord
from orchestrator.strategy_loader import load_strategy
from config import CONFIG
import time

class RayExecutor:

    @staticmethod
    def _load_stage_functions(stage_names: List[str], db=None) -> Dict[str, Any]:
        """批量加载阶段函数，可选传入已有 db session"""
        from app.models import Stage
        import os
        import importlib.util
        own_db = False
        if db is None:
            from app.database import SessionLocal
            db = SessionLocal()
            own_db = True
        try:
            stages = db.query(Stage).filter(Stage.name.in_(stage_names)).all()
            stage_map = {s.name: s for s in stages}
        finally:
            if own_db:
                db.close()

        result = {}
        for name in stage_names:
            stage_info = stage_map.get(name)
            if not stage_info:
                raise ValueError(f"Stage '{name}' not found in database")
            module_name, func_name = stage_info.handler.split(":")
            module_path = os.path.join(CONFIG.STAGED_CODE_DIR, f"{module_name}.py")
            if not os.path.exists(module_path):
                raise FileNotFoundError(f"Stage code file not found: {module_path}")
            spec = importlib.util.spec_from_file_location(module_name, module_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            if not hasattr(module, func_name):
                raise AttributeError(f"Function '{func_name}' not found in module '{module_name}'")
            result[name] = getattr(module, func_name)
        return result

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
    def _get_deployment_config(stage_name: str, db=None) -> Dict:
        """获取阶段的部署配置"""
        if db is None:
            from app.database import SessionLocal
            db = SessionLocal()
            own_session = True
        else:
            own_session = False
        try:
            from app.models import DeploymentConfig
            config = db.query(DeploymentConfig).filter(DeploymentConfig.stage_name == stage_name).first()
            if not config:
                return {
                    "allowed_tiers": ["edge"],
                    "resources": {"cpu_cores": 0.5, "memory_mb": 128}
                }
            return {
                "allowed_tiers": config.allowed_tiers,
                "resources": config.resources,
                "node_affinity": config.node_affinity,
                "proximity": config.proximity,
            }
        finally:
            if own_session:
                db.close()

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
    def _predict_resources(stage_input: Any, stage_info: Dict) -> Optional[Dict]:
        """使用预测器预测阶段所需资源，返回 {cpu_cores, memory_mb} 或 None"""
        from config import CONFIG
        if not CONFIG.USE_RESOURCE_PREDICTOR:
            return None
        input_type = stage_info.get("input_type") if stage_info else None
        if not input_type or input_type not in ("image", "video", "data"):
            return None
        raw = {}
        if isinstance(stage_input, dict):
            fc = stage_input.get("file_content")
            meta = stage_input.get("metadata", {})
            if fc and input_type == "image":
                try:
                    from PIL import Image
                    import io
                    img = Image.open(io.BytesIO(fc if isinstance(fc, bytes) else fc))
                    raw["width"], raw["height"] = img.size
                    raw["channels"] = len(img.getbands())
                    raw["file_size_kb"] = len(fc) / 1024 if isinstance(fc, bytes) else 0
                except Exception:
                    pass
            raw.update({k: v for k, v in meta.items() if k in (
                "width", "height", "channels", "file_size_kb",
                "duration_s", "fps", "bitrate_kbps",
                "record_count", "field_count", "total_size_kb", "nesting_depth")})
        if not raw:
            return None
        try:
            from predictor.predict import predict
            res = predict(input_type, raw)
            return {"cpu_cores": max(0.1, res["cpu_percent"] / 100),
                    "memory_mb": int(max(16, res["memory_mb"]))}
        except Exception:
            return None

    @staticmethod
    def _execute_stage(stage_name: str, stage_input: Any, target_tier: str,
                       stage_func=None, db=None, stage_info: Dict = None,
                       target_node: str = None) -> Dict:
        """执行单个阶段，返回执行结果。

        Args:
            target_node: 策略指定的 Ray hex node ID，如果提供则使用节点亲和调度。
        """
        deploy_config = RayExecutor._get_deployment_config(stage_name, db)

        # 注入 stage config 到输入中
        if stage_info:
            sc = stage_info.get("config")
            if sc and isinstance(stage_input, dict):
                stage_input = {**stage_input, "_stage_config": sc}

        allowed_tiers = deploy_config.get("allowed_tiers", ["end", "edge", "cloud"])
        if not RayExecutor._validate_tier(target_tier, allowed_tiers):
            if allowed_tiers:
                fallback_tier = allowed_tiers[0]
                print(f"⚠️ 请求层级 '{target_tier}' 不在允许列表中，使用默认层级 '{fallback_tier}'")
                target_tier = fallback_tier
            else:
                target_tier = "edge"

        # 资源预测器覆盖（仅覆盖 cpu/memory，不覆盖 allowed_tiers）
        predicted = RayExecutor._predict_resources(stage_input, stage_info)
        if predicted:
            print(f"[预测] {stage_name} → CPU={predicted['cpu_cores']:.1f}核 MEM={predicted['memory_mb']}MB")
            deploy_config["resources"] = {**deploy_config.get("resources", {}), **predicted}

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

        node_affinity = deploy_config.get("node_affinity")
        if node_affinity:
            match_labels = node_affinity.get("match_labels", {}) if isinstance(node_affinity, dict) else {}
            for label_key, label_val in match_labels.items():
                ray_options["resources"][f"label_{label_key}_{label_val}"] = 0.001

        proximity = deploy_config.get("proximity")
        if proximity:
            target = proximity.get("target_stage") if isinstance(proximity, dict) else None
            ptype = proximity.get("proximity_type") if isinstance(proximity, dict) else None
            print(f"[调度] 邻近需求: stage={stage_name} 靠近 {target} ({ptype})")

        # 构建 Ray runtime_env: 合并 dependencies 和 runtime_env
        ray_runtime_env = {}
        if stage_info:
            deps = stage_info.get("dependencies")
            if deps and isinstance(deps, list) and len(deps) > 0:
                ray_runtime_env["pip"] = deps
            renv = stage_info.get("runtime_env")
            if renv and isinstance(renv, dict):
                for k, v in renv.items():
                    if k != "pip":
                        ray_runtime_env[k] = v
        if ray_runtime_env:
            ray_options["runtime_env"] = ray_runtime_env

        from config import CONFIG

        # 节点亲和调度：策略指定了 target_node 时，优先调度到该节点
        if target_node and not CONFIG.LOCAL_MODE:
            from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
            ray_options["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                node_id=target_node, soft=True
            )
            print(f"[调度] 节点亲和: target_node={target_node[:12]}... (soft=True)")

        if stage_func is None:
            raise ValueError(f"No stage function provided for '{stage_name}'")
        wrapped_func = RayExecutor._wrap_stage_func(stage_func, target_tier)

        if CONFIG.LOCAL_MODE:
            start = datetime.utcnow()
            before_call = time.perf_counter()
            try:
                output_package = wrapped_func(stage_input)
                after_call = time.perf_counter()
                end = datetime.utcnow()
                exec_time_ms = (after_call - before_call) * 1000

                output_data = output_package["result"]
                node_info = output_package["node_info"]

                node_id = node_info.get("worker_id") or node_info.get("hostname", "unknown")
                real_tier = node_info.get("tier", "unknown")
                if real_tier == "unknown":
                    real_tier = target_tier

                input_size = len(str(stage_input)) if stage_input else 0
                output_size = len(str(output_data)) if output_data else 0

                # 更新节点负载
                if db and node_id:
                    from service.node_info_service import NodeInfoService
                    NodeInfoService._update_load_db(db, node_id,
                        cpu_percent=node_info.get("_cpu_percent"),
                        memory_percent=node_info.get("_memory_mb"))

                return {
                    "result": output_data,
                    "node_id": node_id,
                    "ray_node_id": node_info.get("ray_node_id"),
                    "node_ip": node_info.get("node_ip"),
                    "node_tier": real_tier,
                    "execution_time_ms": exec_time_ms,
                    "transfer_time_ms": 0.0,
                    "input_size_bytes": input_size,
                    "output_size_bytes": output_size,
                    "cpu_percent": node_info.get("_cpu_percent"),
                    "memory_mb": node_info.get("_memory_mb"),
                    "start_time": start,
                    "end_time": end,
                    "success": True
                }
            except Exception as e:
                return {"error": str(e), "success": False}

        import ray
        remote_func = ray.remote(wrapped_func).options(**ray_options)

        start = datetime.utcnow()
        before_remote = time.perf_counter()
        try:
            obj_ref = remote_func.remote(stage_input)
            output_package = ray.get(obj_ref)
            output_data = output_package["result"]
            node_info = output_package["node_info"]
            after_get = time.perf_counter()
            end = datetime.utcnow()

            # 实际执行时间由 wrapper 测量并返回
            actual_exec_ms = node_info.get("_execution_time_ms")
            total_roundtrip_ms = (after_get - before_remote) * 1000
            if actual_exec_ms is not None:
                exec_time_ms = actual_exec_ms
                transfer_time_ms = max(0.0, total_roundtrip_ms - actual_exec_ms)
            else:
                exec_time_ms = total_roundtrip_ms
                transfer_time_ms = 0.0

            node_id = node_info.get("ray_node_id") or node_info.get("worker_id") or node_info.get("hostname", "unknown")
            real_tier = node_info.get("tier", "unknown")
            if real_tier == "unknown":
                real_tier = target_tier

            input_size = len(str(stage_input)) if stage_input else 0
            output_size = len(str(output_data)) if output_data else 0

            # 更新节点负载
            if db and node_id:
                from service.node_info_service import NodeInfoService
                NodeInfoService._update_load_db(db, node_id,
                    cpu_percent=node_info.get("_cpu_percent"),
                    memory_percent=node_info.get("_memory_mb"))

            return {
                "result": output_data,
                "node_id": node_id,
                "ray_node_id": node_info.get("ray_node_id"),
                "node_ip": node_info.get("node_ip"),
                "node_tier": real_tier,
                "execution_time_ms": exec_time_ms,
                "transfer_time_ms": transfer_time_ms,
                "input_size_bytes": input_size,
                "output_size_bytes": output_size,
                "cpu_percent": node_info.get("_cpu_percent"),
                "memory_mb": node_info.get("_memory_mb"),
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
    def _wrap_stage_func(stage_func, expected_tier: str = "unknown"):
        """包装阶段函数，添加节点信息及执行时延/资源测量"""
        def wrapper(input_data):
            import socket
            import time
            import os

            hostname = socket.gethostname()
            node_ip = None
            ray_node_id = None

            try:
                import ray
                ray_node_id = ray.get_runtime_context().get_node_id()
                # 获取节点 IP：从 Ray 节点信息中查找
                for n in ray.nodes():
                    if n["NodeID"] == ray_node_id:
                        node_ip = n.get("NodeManagerAddress", "")
                        break
            except Exception:
                pass

            # node_id 优先用 Ray hex ID，回退到 hostname
            node_id = ray_node_id or hostname

            # 执行前采样：内存 + CPU 时间
            exec_before = time.perf_counter()
            utime_before = stime_before = None
            mem_mb = None
            try:
                with open(f"/proc/{os.getpid()}/status") as f:
                    for line in f:
                        if line.startswith("VmRSS:"):
                            mem_mb = int(line.split()[1]) // 1024
                            break
            except:
                pass
            try:
                with open(f"/proc/{os.getpid()}/stat") as f:
                    line = f.read()
                idx = line.rfind(")")
                stat_fields = line[idx+1:].split()
                utime_before = int(stat_fields[11])
                stime_before = int(stat_fields[12])
            except:
                pass

            result = stage_func(input_data)

            # 执行后采样
            exec_after = time.perf_counter()
            exec_time_ms = (exec_after - exec_before) * 1000
            mem_after_mb = None
            utime_after = stime_after = None
            try:
                with open(f"/proc/{os.getpid()}/status") as f:
                    for line in f:
                        if line.startswith("VmRSS:"):
                            mem_after_mb = int(line.split()[1]) // 1024
                            break
            except:
                pass
            try:
                with open(f"/proc/{os.getpid()}/stat") as f:
                    line = f.read()
                idx = line.rfind(")")
                stat_fields = line[idx+1:].split()
                utime_after = int(stat_fields[11])
                stime_after = int(stat_fields[12])
            except:
                pass

            # CPU 使用率：进程 CPU 时间 / 墙上时间
            cpu_percent = None
            if None not in (utime_before, utime_after, stime_before, stime_after) and exec_time_ms > 0:
                try:
                    clk_tck = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
                except:
                    clk_tck = 100
                cpu_ticks = (utime_after + stime_after - utime_before - stime_before)
                cpu_time_ms = cpu_ticks * (1000.0 / clk_tck)
                cpu_percent = min(100.0, cpu_time_ms / exec_time_ms * 100)

            node_info = {
                "hostname": hostname,
                "node_id": str(node_id),
                "ray_node_id": ray_node_id,
                "node_ip": node_ip,
                "tier": expected_tier,
                "_execution_time_ms": exec_time_ms,
                "_cpu_percent": cpu_percent,
                "_memory_mb": mem_after_mb or mem_mb,
            }

            return {
                "result": result,
                "node_info": node_info
            }
        return wrapper

    @staticmethod
    def _prepare_stage_input(current_input: Any, current_stage: str) -> Any:
        """准备阶段的输入数据"""
        stage_input = current_input
        print(f"[_prepare_stage_input] 原始输入: type={type(current_input)}, value={repr(current_input)}")

        if isinstance(current_input, str):
            print(f"[_prepare_stage_input] 裸字符串输入，转为 {{'file_id': ...}}")
            current_input = {"file_id": current_input}
            stage_input = current_input

        if isinstance(current_input, dict) and "file_id" in current_input:
            print(f"[_prepare_stage_input] 检测到 file_id!")
            try:
                from service.file_service import FileService
                file_id = current_input["file_id"]
                
                # 先检查 file_info
                print(f"[_prepare_stage_input] 准备调用 FileService.get_file({file_id})")
                file_info = FileService.get_file(file_id)
                print(f"[_prepare_stage_input] file_info: {file_info}")
                
                file_content = FileService.get_file_content(file_id)
                print(f"[_prepare_stage_input] file_content: type={type(file_content)}, len={len(file_content) if file_content else 0}")
                
                if file_content:
                    stage_input = {
                        "file_content": file_content,
                        "file_id": file_id,
                        "metadata": current_input.get("metadata", {})
                    }
                    print(f"[_prepare_stage_input] ✅ 文件加载成功! file_id={file_id}, size={len(file_content)} bytes")
                else:
                    print(f"[_prepare_stage_input] ❌ 文件内容为空! file_id={file_id}")
            except Exception as e:
                import traceback
                print(f"[_prepare_stage_input] ❌ 发生异常! {type(e)}: {e}")
                print(f"[_prepare_stage_input] 异常堆栈:\n{traceback.format_exc()}")
        else:
            print(f"[_prepare_stage_input] 不是 dict 或没有 file_id")
            
        return stage_input

    @staticmethod
    def _get_app_dict(app_name: str, db=None) -> Dict:
        """从数据库获取应用配置"""
        if db is None:
            from app.database import SessionLocal
            db = SessionLocal()
            own_session = True
        else:
            own_session = False
        try:
            from app.models import Application, ApplicationStage, ApplicationEdge, ApplicationEntry, ApplicationExit, Stage as StageModel
            
            app = db.query(Application).filter(Application.name == app_name).first()
            if not app:
                raise ValueError(f"Application '{app_name}' not found")
            
            stages = db.query(ApplicationStage).filter(ApplicationStage.app_id == app.app_id).order_by(ApplicationStage.order_index).all()
            edges = db.query(ApplicationEdge).filter(ApplicationEdge.app_id == app.app_id).all()
            entries = db.query(ApplicationEntry).filter(ApplicationEntry.app_id == app.app_id).all()
            exits = db.query(ApplicationExit).filter(ApplicationExit.app_id == app.app_id).all()
            
            # 批量查询 stage 信息，消除 N+1
            stage_names = [s.stage_name for s in stages]
            stage_rows = db.query(StageModel).filter(StageModel.name.in_(stage_names)).all() if stage_names else []
            from app.models import Model as ModelRecord
            model_rows = db.query(ModelRecord).filter(ModelRecord.stage_name.in_(stage_names)).all() if stage_names else []
            models_by_stage = {}
            for m in model_rows:
                models_by_stage.setdefault(m.stage_name, []).append({
                    "model_id": m.model_id,
                    "name": m.name,
                    "version": m.version,
                    "load_method": m.load_method,
                    "inference_config": m.inference_config,
                    "alternative_models": m.alternative_models,
                })

            stage_info = {}
            for st in stage_rows:
                stage_info[st.name] = {
                    "input_type": st.input_type,
                    "output_type": st.output_type,
                    "can_split": st.can_split,
                    "parent_stage": st.parent_stage,
                    "split_config": st.config,
                    "config": st.config,
                    "dependencies": st.dependencies,
                    "runtime_env": st.runtime_env,
                    "model_name": st.model_name,
                    "models": models_by_stage.get(st.name, []),
                }

            return {
                "app_id": app.app_id,
                "name": app.name,
                "description": app.description,
                "input_type": app.input_type,
                "stages": stage_names,
                "stage_info": stage_info,
                "edges": [
                    {"from_stage": e.from_stage, "to_stage": e.to_stage,
                     "is_split_point": e.is_split_point if hasattr(e, 'is_split_point') else False}
                    for e in edges
                ],
                "entry_stage": entries[0].stage_name if entries else None,
                "exit_stages": [e.stage_name for e in exits]
            }
        finally:
            if own_session:
                db.close()
    
    @staticmethod
    def _get_available_nodes(db) -> List[Dict]:
        from service.node_info_service import NodeInfoService
        return NodeInfoService._list_nodes_db(db)

    @staticmethod
    def _select_best_node(db, target_tier: str, target_node: str = None) -> Optional[str]:
        """选择最佳 Ray 节点，返回 Ray hex node ID。

        1. 如果策略指定了 target_node 且它在目标层级且健康 → 使用它
        2. 否则按 CPU×0.5 + 内存×0.5 加权评分选最低负载节点
        3. 无可用节点时返回 None（回退到 Ray 默认调度）
        """
        from service.node_info_service import NodeInfoService
        nodes = NodeInfoService._list_nodes_db(db)
        tier_nodes = [n for n in nodes if n.get("tier") == target_tier]

        if not tier_nodes:
            return None

        if target_node:
            match = next((n for n in tier_nodes if n["node_id"] == target_node), None)
            if match and (match.get("current_cpu_percent") or 0) < 95:
                return target_node
            print(f"[调度] 指定节点 {target_node[:12]}... 不可用，回退到最优选择")

        healthy = [n for n in tier_nodes if (n.get("current_cpu_percent") or 0) < 95]
        if not healthy:
            healthy = tier_nodes

        def load_score(n):
            cpu = n.get("current_cpu_percent") or 0
            mem = n.get("current_memory_percent") or 0
            return cpu * 0.5 + mem * 0.5

        best = min(healthy, key=load_score)
        best_id = best.get("node_id")
        print(f"[调度] 最优节点: {best_id[:12] if best_id else '?'}... "
              f"(CPU={best.get('current_cpu_percent', '?')}%, MEM={best.get('current_memory_percent', '?')}%)")
        return best_id

    @staticmethod
    def _lookup_sub_stages(db, parent_stage_name: str) -> List[Dict]:
        from app.models import Stage
        sub_stages = db.query(Stage).filter(Stage.parent_stage == parent_stage_name).order_by(Stage.name).all()
        return [{"name": s.name, "handler": s.handler} for s in sub_stages]

    @staticmethod
    def _split_input(data: Any, split_plan: Dict) -> List:
        """按 split_plan 将输入切分"""
        count = split_plan.get("count", 1)
        if isinstance(data, dict) and "file_content" in data:
            fc = data["file_content"]
            chunk_size = max(1, len(fc) // count)
            parts = []
            for i in range(count):
                chunk = fc[i * chunk_size:(i + 1) * chunk_size] if i < count - 1 else fc[i * chunk_size:]
                part = {**data, "file_content": chunk, "_split_index": i}
                parts.append(part)
            return parts
        if isinstance(data, list):
            chunk_size = max(1, len(data) // count)
            return [data[i * chunk_size:(i + 1) * chunk_size] for i in range(count)]
        return [data] * count

    @staticmethod
    def _merge_outputs(results: List[Dict]) -> Dict:
        """合并切分执行结果"""
        file_contents = [r.get("file_content", b"") for r in results if isinstance(r, dict)]
        if file_contents and any(file_contents):
            merged = b"".join(fc for fc in file_contents if fc)
            merged_meta = {}
            for r in results:
                m = r.get("metadata", {})
                if m:
                    merged_meta.update(m)
            return {"file_content": merged, "metadata": merged_meta, "_merged": True}
        return {"_merged": True, "splits": results}

    @staticmethod
    def execute(task_id: str, app_name: str, strategy_name: str, input_data: Any, runtime_config: Optional[Dict] = None) -> Dict:
        from app.database import SessionLocal
        db = SessionLocal()
        try:
            app_dict = RayExecutor._get_app_dict(app_name, db)

            strategy = load_strategy(strategy_name, db)

            # 批量预加载所有阶段的函数
            all_stage_names = app_dict.get("stages", [])
            exit_stages = app_dict.get("exit_stages", [])
            stage_funcs = RayExecutor._load_stage_functions(all_stage_names, db)

            trace = ExecutionTraceSchema(task_id=task_id)
            step_index = 0
            current_stage = app_dict["entry_stage"]
            current_input = input_data
            final_output = None

            while True:
                possible_next = RayExecutor._get_next_stages(app_dict, current_stage)
                is_exit = current_stage in exit_stages

                has_split_point = False
                stage_can_split = False
                stage_info = app_dict.get("stage_info", {})
                cur_info = stage_info.get(current_stage, {})
                stage_can_split = cur_info.get("can_split", False)
                for e in app_dict.get("edges", []):
                    if e["from_stage"] == current_stage and e.get("is_split_point"):
                        has_split_point = True
                        break

                context = {
                    "current_stage": current_stage,
                    "input": current_input,
                    "possible_next_stages": possible_next,
                    "execution_history": [s.dict() for s in trace.execution_path],
                    "runtime_config": runtime_config,
                    "is_split_point": has_split_point,
                    "stage_can_split": stage_can_split,
                    "stage_models": cur_info.get("models", []),
                    "available_nodes": RayExecutor._get_available_nodes(db),
                }

                print(f"[决策] 当前阶段={current_stage}, 可选下一步={possible_next}, is_exit={is_exit}")

                if is_exit or not possible_next:
                    print(f"[执行] 运行出口阶段: {current_stage}")
                    stage_input = RayExecutor._prepare_stage_input(current_input, current_stage)
                    deploy_config = RayExecutor._get_deployment_config(current_stage, db)
                    target_tier = deploy_config.get("allowed_tiers", ["edge"])[0]
                    target_node = RayExecutor._select_best_node(db, target_tier)
                    exec_result = RayExecutor._execute_stage(current_stage, stage_input, target_tier, stage_funcs[current_stage], db, cur_info, target_node=target_node)

                    if not exec_result["success"]:
                        error_msg = exec_result.get('error', 'unknown error')
                        if strategy.has_fallback:
                            fb_decision = strategy.decide_fallback(context, {"error": error_msg, "stage": current_stage})
                            if fb_decision.get("action") == "skip" and fb_decision.get("next_stage"):
                                print(f"[回退] 跳过 {current_stage} → {fb_decision['next_stage']}")
                                current_stage = fb_decision["next_stage"]
                                continue
                            elif fb_decision.get("action") == "retry":
                                print(f"[回退] 重试 {current_stage}")
                                continue
                        trace.error_logs.append(f"Stage {current_stage} failed: {error_msg}")
                        raise Exception(error_msg)

                    step = StepRecord(
                        step_index=step_index, stage_name=current_stage,
                        node_id=exec_result["node_id"], node_tier=exec_result["node_tier"],
                        start_time=exec_result["start_time"], end_time=exec_result["end_time"],
                        execution_time_ms=exec_result["execution_time_ms"],
                        transfer_time_ms=exec_result.get("transfer_time_ms", 0.0),
                        input_size_bytes=exec_result.get("input_size_bytes"),
                        output_size_bytes=exec_result.get("output_size_bytes"),
                        cpu_percent=exec_result.get("cpu_percent"),
                        memory_mb=exec_result.get("memory_mb"),
                        ray_node_id=exec_result.get("ray_node_id"),
                        node_ip=exec_result.get("node_ip"),
                    )
                    trace.execution_path.append(step)
                    final_output = exec_result["result"]
                    print(f"[完成] 阶段 '{current_stage}' 是出口阶段，执行结束")
                    break

                if has_split_point and stage_can_split and strategy.has_split:
                    decision = strategy.decide_split(context)
                else:
                    decision = strategy.decide(context)

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
                strategy_target_node = decision.get("target_node")
                target_node = RayExecutor._select_best_node(db, target_tier, strategy_target_node)
                print(f"[调度决策] stage={next_stage}, target_tier={target_tier}, target_node={target_node[:12] if target_node else 'auto'}...")

                stage_input = RayExecutor._prepare_stage_input(current_input, current_stage)

                split_plan = decision.get("split_plan") if has_split_point and stage_can_split else None
                if split_plan:
                    print(f"[切分] 切分执行 {current_stage}, count={split_plan.get('count', 1)}")
                    parts = RayExecutor._split_input(stage_input, split_plan)
                    sub_stages = RayExecutor._lookup_sub_stages(db, current_stage)
                    from config import CONFIG

                    if sub_stages:
                        print(f"[切分] 发现 {len(sub_stages)} 个子阶段: {[s['name'] for s in sub_stages]}")
                        sub_funcs = RayExecutor._load_stage_functions([s['name'] for s in sub_stages], db)
                        chunk_results = []
                        total_exec = 0.0
                        for i, part in enumerate(parts):
                            data = part
                            for s in sub_stages:
                                sf = sub_funcs[s['name']]
                                w = RayExecutor._wrap_stage_func(sf, target_tier)
                                wrapped_out = w(data)
                                data = wrapped_out["result"]
                                total_exec += wrapped_out.get("node_info", {}).get("_execution_time_ms", 0)
                            chunk_results.append(data)
                        outputs = chunk_results
                        exec_time_sum = total_exec
                    else:
                        remote_func_raw = stage_funcs[current_stage]
                        wrapped = RayExecutor._wrap_stage_func(remote_func_raw, target_tier)
                        deploy_config = RayExecutor._get_deployment_config(current_stage, db)

                        if CONFIG.LOCAL_MODE:
                            wrapped_results = [wrapped(p) for p in parts]
                            outputs = [r["result"] for r in wrapped_results]
                            exec_time_sum = sum(r.get("node_info", {}).get("_execution_time_ms", 0) for r in wrapped_results)
                        else:
                            import ray
                            tier_resource = RayExecutor._get_tier_resource_name(target_tier)
                            ray_opts = {
                                "num_cpus": deploy_config.get("resources", {}).get("cpu_cores", 0.5),
                                "resources": {tier_resource: 1},
                            }
                            remote = ray.remote(wrapped).options(**ray_opts)
                            refs = [remote.remote(p) for p in parts]
                            split_results = ray.get(refs)
                            outputs = [r["result"] for r in split_results]
                            exec_time_sum = sum(r.get("node_info", {}).get("_execution_time_ms", 0) for r in split_results)

                    merged_output = RayExecutor._merge_outputs(outputs)
                    exec_result = {
                        "result": merged_output,
                        "node_id": "split_merged",
                        "node_tier": target_tier,
                        "start_time": datetime.utcnow(),
                        "end_time": datetime.utcnow(),
                        "execution_time_ms": exec_time_sum,
                        "transfer_time_ms": 0.0,
                        "success": True,
                    }
                else:
                    print(f"[执行] 运行阶段: {current_stage} -> {next_stage}")
                    exec_result = RayExecutor._execute_stage(current_stage, stage_input, target_tier, stage_funcs[current_stage], db, cur_info, target_node=target_node)

                if not exec_result["success"]:
                    error_msg = exec_result.get('error', 'unknown error')
                    if strategy.has_fallback:
                        fb_decision = strategy.decide_fallback(context, {"error": error_msg, "stage": current_stage})
                        if fb_decision.get("action") == "skip" and fb_decision.get("next_stage"):
                            print(f"[回退] 跳过 {current_stage} → {fb_decision['next_stage']}")
                            current_stage = fb_decision["next_stage"]
                            continue
                        elif fb_decision.get("action") == "retry":
                            print(f"[回退] 重试 {current_stage}")
                            continue
                    trace.error_logs.append(f"Stage {current_stage} failed: {error_msg}")
                    raise Exception(error_msg)

                step = StepRecord(
                    step_index=step_index, stage_name=current_stage,
                    node_id=exec_result["node_id"], node_tier=exec_result["node_tier"],
                    start_time=exec_result["start_time"], end_time=exec_result["end_time"],
                    execution_time_ms=exec_result["execution_time_ms"],
                    transfer_time_ms=exec_result.get("transfer_time_ms", 0.0),
                    input_size_bytes=exec_result.get("input_size_bytes"),
                    output_size_bytes=exec_result.get("output_size_bytes"),
                    cpu_percent=exec_result.get("cpu_percent"),
                    memory_mb=exec_result.get("memory_mb"),
                    ray_node_id=exec_result.get("ray_node_id"),
                    node_ip=exec_result.get("node_ip"),
                )
                trace.execution_path.append(step)
                current_input = exec_result["result"]

                # 数据变换
                stage_info = app_dict.get("stage_info", {})
                cur_info = stage_info.get(current_stage, {})
                next_info = stage_info.get(next_stage, {})
                cur_out = cur_info.get("output_type")
                next_in = next_info.get("input_type")
                if cur_out and next_in and cur_out != next_in:
                    from service.data_transform_service import DataTransformService
                    transform = DataTransformService._find_transform(db, cur_out, next_in)
                    if transform:
                        print(f"[变换] {cur_out} → {next_in} via {transform.name}")
                        current_input = DataTransformService.apply_transform(
                            transform.handler, current_input, transform.config
                        )
                    else:
                        raise ValueError(
                            f"Type mismatch: '{current_stage}' outputs '{cur_out}', "
                            f"but '{next_stage}' expects '{next_in}'. "
                            f"No DataTransform registered for '{cur_out}' → '{next_in}'"
                        )

                current_stage = next_stage
                step_index += 1

            trace.total_latency_ms = sum(s.execution_time_ms for s in trace.execution_path)
            return {
                "final_output": final_output,
                "trace": trace.dict(),
                "status": TaskStatus.COMPLETED.value
            }
        finally:
            db.close()
