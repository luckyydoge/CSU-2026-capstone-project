#!/usr/bin/env python3
"""
策略注册脚本
注册debug_end, debug_edge, debug_cloud, random策略
"""

import requests
import json

BASE_URL = "http://127.0.0.1:8001/api/v1"

def register_strategies():
    """注册策略"""
    print("=== 注册策略 ===")

    strategies = [
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
        },
        {
            "name": "random",
            "strategy_type": "routing",
            "handler": "random_routing:decide",
            "description": "随机路由策略"
        }
    ]

    for strategy in strategies:
        try:
            response = requests.post(f"{BASE_URL}/strategies", json=strategy, timeout=10)
            print(f"  {strategy['name']}: {response.status_code}")
            if response.status_code != 201:
                print(f"    响应: {response.text}")
        except Exception as e:
            print(f"  {strategy['name']}: 错误 - {str(e)}")

    print("\n✅ 策略注册完成")

def main():
    """主函数"""
    print("=== 开始策略注册 ===\n")

    try:
        register_strategies()
        print("\n=== 策略注册完成 ===")

    except Exception as e:
        print(f"\n注册过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
