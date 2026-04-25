#!/usr/bin/env python3
"""
资源注册脚本
注册多个阶段、不同的部署配置和多个应用
"""

import requests
import json
import time

BASE_URL = "http://127.0.0.1:8001/api/v1"

def register_stages():
    """注册多个阶段"""
    print("=== 注册阶段 ===")
    
    stages = [
        {
            "name": "process",
            "handler": "process:run",
            "input_type": "json",
            "output_type": "json",
            "description": "基础处理阶段"
        },
        {
            "name": "cpuprocess",
            "handler": "cpuprocess:run",
            "input_type": "json",
            "output_type": "json",
            "description": "CPU密集型处理阶段"
        },
        {
            "name": "memoprocess",
            "handler": "memoprocess:run",
            "input_type": "json",
            "output_type": "json",
            "description": "内存密集型处理阶段"
        },
        {
            "name": "output",
            "handler": "output:run",
            "input_type": "json",
            "output_type": "json",
            "description": "输出阶段"
        },
        {
            "name": "end_process",
            "handler": "process:run",
            "input_type": "json",
            "output_type": "json",
            "description": "End节点处理阶段"
        },
        {
            "name": "edge_process",
            "handler": "process:run",
            "input_type": "json",
            "output_type": "json",
            "description": "Edge节点处理阶段"
        },
        {
            "name": "cloud_process",
            "handler": "process:run",
            "input_type": "json",
            "output_type": "json",
            "description": "Cloud节点处理阶段"
        }
    ]
    
    for stage in stages:
        try:
            response = requests.post(f"{BASE_URL}/stages", json=stage, timeout=10)
            print(f"  {stage['name']}: {response.status_code}")
        except Exception as e:
            print(f"  {stage['name']}: 错误 - {str(e)}")
    
    print("\n✅ 阶段注册完成")

def register_deployments():
    """注册不同的部署配置"""
    print("\n=== 注册部署配置 ===")
    
    deployments = [
        # 基础部署配置
        {
            "stage_name": "process",
            "allowed_tiers": ["end", "edge", "cloud"],
            "resources": {"cpu_cores": 0.1, "memory_mb": 64},
            "description": "基础处理部署，低资源消耗"
        },
        # CPU密集型部署配置
        {
            "stage_name": "cpuprocess",
            "allowed_tiers": ["edge", "cloud"],
            "resources": {"cpu_cores": 1.0, "memory_mb": 256},
            "description": "CPU密集型部署，高CPU消耗"
        },
        # 内存密集型部署配置
        {
            "stage_name": "memoprocess",
            "allowed_tiers": ["end", "cloud"],
            "resources": {"cpu_cores": 0.5, "memory_mb": 512},
            "description": "内存密集型部署，高内存消耗"
        },
        # 输出阶段部署配置
        {
            "stage_name": "output",
            "allowed_tiers": ["cloud"],
            "resources": {"cpu_cores": 0.2, "memory_mb": 128},
            "description": "输出阶段部署，仅云节点"
        },
        # End节点专属部署
        {
            "stage_name": "end_process",
            "allowed_tiers": ["end"],
            "resources": {"cpu_cores": 0.5, "memory_mb": 128},
            "description": "End节点专属部署"
        },
        # Edge节点专属部署
        {
            "stage_name": "edge_process",
            "allowed_tiers": ["edge"],
            "resources": {"cpu_cores": 0.5, "memory_mb": 128},
            "description": "Edge节点专属部署"
        },
        # Cloud节点专属部署
        {
            "stage_name": "cloud_process",
            "allowed_tiers": ["cloud"],
            "resources": {"cpu_cores": 0.5, "memory_mb": 128},
            "description": "Cloud节点专属部署"
        }
    ]
    
    for deployment in deployments:
        try:
            response = requests.post(f"{BASE_URL}/deployments", json=deployment, timeout=10)
            print(f"  {deployment['stage_name']}: {response.status_code}")
        except Exception as e:
            print(f"  {deployment['stage_name']}: 错误 - {str(e)}")
    
    print("\n✅ 部署配置注册完成")

def register_strategies():
    """注册策略"""
    print("\n=== 注册策略 ===")
    
    strategies = [
        {
            "name": "random",
            "strategy_type": "routing",
            "handler": "random_routing:decide",
            "description": "随机路由策略"
        },
        {
            "name": "debug_end",
            "strategy_type": "routing",
            "handler": "debug_strategy:debug_strategy_end",
            "description": "强制调度到end节点"
        },
        {
            "name": "debug_edge",
            "strategy_type": "routing",
            "handler": "debug_strategy:debug_strategy_edge",
            "description": "强制调度到edge节点"
        },
        {
            "name": "debug_cloud",
            "strategy_type": "routing",
            "handler": "debug_strategy:debug_strategy_cloud",
            "description": "强制调度到cloud节点"
        }
    ]
    
    for strategy in strategies:
        try:
            response = requests.post(f"{BASE_URL}/strategies", json=strategy, timeout=10)
            print(f"  {strategy['name']}: {response.status_code}")
        except Exception as e:
            print(f"  {strategy['name']}: 错误 - {str(e)}")
    
    print("\n✅ 策略注册完成")

def register_applications():
    """注册多个应用"""
    print("\n=== 注册应用 ===")
    
    applications = [
        # 基础应用：process → output
        {
            "name": "basic_app",
            "description": "基础应用流程",
            "input_type": "json",
            "stages": [
                {"name": "process", "output_type": "json"},
                {"name": "output", "output_type": "json"}
            ],
            "edges": [
                {"from_stage": "process", "to_stage": "output"}
            ],
            "entry_stage": "process",
            "exit_stages": ["output"]
        },
        # 分支应用：process → cpuprocess/memoprocess → output
        {
            "name": "branch_app",
            "description": "分支应用流程",
            "input_type": "json",
            "stages": [
                {"name": "process", "output_type": "json"},
                {"name": "cpuprocess", "output_type": "json"},
                {"name": "memoprocess", "output_type": "json"},
                {"name": "output", "output_type": "json"}
            ],
            "edges": [
                {"from_stage": "process", "to_stage": "cpuprocess"},
                {"from_stage": "process", "to_stage": "memoprocess"},
                {"from_stage": "cpuprocess", "to_stage": "output"},
                {"from_stage": "memoprocess", "to_stage": "output"}
            ],
            "entry_stage": "process",
            "exit_stages": ["output"]
        },
        # 分层应用：end_process → edge_process → cloud_process
        {
            "name": "tier_app",
            "description": "分层应用流程",
            "input_type": "json",
            "stages": [
                {"name": "end_process", "output_type": "json"},
                {"name": "edge_process", "output_type": "json"},
                {"name": "cloud_process", "output_type": "json"}
            ],
            "edges": [
                {"from_stage": "end_process", "to_stage": "edge_process"},
                {"from_stage": "edge_process", "to_stage": "cloud_process"}
            ],
            "entry_stage": "end_process",
            "exit_stages": ["cloud_process"]
        },
        # 混合应用：process → cpuprocess → output
        {
            "name": "cpu_app",
            "description": "CPU密集型应用流程",
            "input_type": "json",
            "stages": [
                {"name": "process", "output_type": "json"},
                {"name": "cpuprocess", "output_type": "json"},
                {"name": "output", "output_type": "json"}
            ],
            "edges": [
                {"from_stage": "process", "to_stage": "cpuprocess"},
                {"from_stage": "cpuprocess", "to_stage": "output"}
            ],
            "entry_stage": "process",
            "exit_stages": ["output"]
        },
        # 混合应用：process → memoprocess → output
        {
            "name": "memo_app",
            "description": "内存密集型应用流程",
            "input_type": "json",
            "stages": [
                {"name": "process", "output_type": "json"},
                {"name": "memoprocess", "output_type": "json"},
                {"name": "output", "output_type": "json"}
            ],
            "edges": [
                {"from_stage": "process", "to_stage": "memoprocess"},
                {"from_stage": "memoprocess", "to_stage": "output"}
            ],
            "entry_stage": "process",
            "exit_stages": ["output"]
        }
    ]
    
    for app in applications:
        try:
            response = requests.post(f"{BASE_URL}/applications", json=app, timeout=10)
            print(f"  {app['name']}: {response.status_code}")
        except Exception as e:
            print(f"  {app['name']}: 错误 - {str(e)}")
    
    print("\n✅ 应用注册完成")

def verify_resources():
    """验证资源注册情况"""
    print("\n=== 验证资源注册 ===")
    
    # 验证阶段
    print("\n验证阶段:")
    try:
        response = requests.get(f"{BASE_URL}/stages", timeout=10)
        if response.status_code == 200:
            stages = response.json()
            print(f"  已注册阶段数: {len(stages)}")
            for stage in stages:
                print(f"  - {stage}")
        else:
            print(f"  获取阶段失败: {response.status_code}")
    except Exception as e:
        print(f"  验证阶段时出错: {str(e)}")
    
    # 验证部署配置
    print("\n验证部署配置:")
    try:
        response = requests.get(f"{BASE_URL}/deployments", timeout=10)
        if response.status_code == 200:
            deployments = response.json()
            print(f"  已注册部署配置数: {len(deployments)}")
            for stage_name, config in deployments.items():
                print(f"  - {stage_name}: {config['allowed_tiers']}")
        else:
            print(f"  获取部署配置失败: {response.status_code}")
    except Exception as e:
        print(f"  验证部署配置时出错: {str(e)}")
    
    # 验证策略
    print("\n验证策略:")
    try:
        response = requests.get(f"{BASE_URL}/strategies", timeout=10)
        if response.status_code == 200:
            strategies = response.json()
            print(f"  已注册策略数: {len(strategies)}")
            for strategy in strategies:
                print(f"  - {strategy}")
        else:
            print(f"  获取策略失败: {response.status_code}")
    except Exception as e:
        print(f"  验证策略时出错: {str(e)}")
    
    # 验证应用
    print("\n验证应用:")
    try:
        response = requests.get(f"{BASE_URL}/applications", timeout=10)
        if response.status_code == 200:
            applications = response.json()
            print(f"  已注册应用数: {len(applications)}")
            for app in applications:
                print(f"  - {app}")
        else:
            print(f"  获取应用失败: {response.status_code}")
    except Exception as e:
        print(f"  验证应用时出错: {str(e)}")

def main():
    """主函数"""
    print("=== 开始资源注册 ===\n")
    
    try:
        # 1. 注册阶段
        register_stages()
        
        # 2. 注册部署配置
        register_deployments()
        
        # 3. 注册策略
        register_strategies()
        
        # 4. 注册应用
        register_applications()
        
        # 5. 验证资源注册
        verify_resources()
        
        print("\n=== 资源注册完成 ===")
        print("\n注册的资源:")
        print("- 7个阶段")
        print("- 7个部署配置")
        print("- 4个策略")
        print("- 5个应用")
        
    except Exception as e:
        print(f"注册过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
