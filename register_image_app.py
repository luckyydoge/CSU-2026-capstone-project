#!/usr/bin/env python3
"""
图片处理测试资源注册脚本
注册resize_10, resize_20, image_output三个阶段
创建一个图片处理应用
"""

import requests
import json

BASE_URL = "http://127.0.0.1:8001/api/v1"

def register_image_stages():
    """注册图片处理阶段"""
    print("=== 注册图片处理阶段 ===")

    stages = [
        {
            "name": "resize_10",
            "handler": "resize_10:run",
            "input_type": "image",
            "output_type": "image",
            "description": "图片分辨率降低10%"
        },
        {
            "name": "resize_20",
            "handler": "resize_20:run",
            "input_type": "image",
            "output_type": "image",
            "description": "图片分辨率降低20%"
        },
        {
            "name": "image_output",
            "handler": "image_output:run",
            "input_type": "image",
            "output_type": "image",
            "description": "图片输出保存"
        }
    ]

    for stage in stages:
        try:
            response = requests.post(f"{BASE_URL}/stages", json=stage, timeout=10)
            print(f"  {stage['name']}: {response.status_code}")
            if response.status_code != 201:
                print(f"    响应: {response.text}")
        except Exception as e:
            print(f"  {stage['name']}: 错误 - {str(e)}")

    print("\n✅ 阶段注册完成")

def register_deployments():
    """注册部署配置"""
    print("\n=== 注册部署配置 ===")

    deployments = [
        {
            "stage_name": "resize_10",
            "allowed_tiers": ["end", "edge", "cloud"],
            "resources": {"cpu_cores": 0.5, "memory_mb": 512},
            "description": "图片处理，低CPU，较高内存"
        },
        {
            "stage_name": "resize_20",
            "allowed_tiers": ["end", "edge", "cloud"],
            "resources": {"cpu_cores": 0.5, "memory_mb": 512},
            "description": "图片处理，低CPU，较高内存"
        },
        {
            "stage_name": "image_output",
            "allowed_tiers": ["cloud"],
            "resources": {"cpu_cores": 0.2, "memory_mb": 256},
            "description": "图片输出，保存到云节点"
        }
    ]

    for deployment in deployments:
        try:
            response = requests.post(f"{BASE_URL}/deployments", json=deployment, timeout=10)
            print(f"  {deployment['stage_name']}: {response.status_code}")
        except Exception as e:
            print(f"  {deployment['stage_name']}: 错误 - {str(e)}")

    print("\n✅ 部署配置注册完成")

def register_applications():
    """注册图片处理应用"""
    print("\n=== 注册图片处理应用 ===")

    app = {
        "name": "image_process_app",
        "description": "图片处理应用：原始图片 -> 缩小10% -> 缩小20% -> 输出",
        "input_type": "image",
        "stages": [
            {"name": "resize_10", "output_type": "image"},
            {"name": "resize_20", "output_type": "image"},
            {"name": "image_output", "output_type": "image"}
        ],
        "edges": [
            {"from_stage": "resize_10", "to_stage": "resize_20"},
            {"from_stage": "resize_20", "to_stage": "image_output"}
        ],
        "entry_stage": "resize_10",
        "exit_stages": ["image_output"]
    }

    try:
        response = requests.post(f"{BASE_URL}/applications", json=app, timeout=10)
        print(f"  image_process_app: {response.status_code}")
        if response.status_code != 201:
            print(f"    响应: {response.text}")
        else:
            print("  ✅ 应用注册成功")
    except Exception as e:
        print(f"  image_process_app: 错误 - {str(e)}")

    print("\n✅ 应用注册完成")

def verify_resources():
    """验证资源注册"""
    print("\n=== 验证资源注册 ===")

    print("\n已注册阶段:")
    try:
        response = requests.get(f"{BASE_URL}/stages", timeout=10)
        if response.status_code == 200:
            stages = response.json()
            for stage in stages:
                if stage in ["resize_10", "resize_20", "image_output"]:
                    print(f"  ✅ {stage}")
    except Exception as e:
        print(f"  获取阶段失败: {str(e)}")

    print("\n已注册应用:")
    try:
        response = requests.get(f"{BASE_URL}/applications", timeout=10)
        if response.status_code == 200:
            apps = response.json()
            print(f"  应用数量: {len(apps)}")
    except Exception as e:
        print(f"  获取应用失败: {str(e)}")

def main():
    """主函数"""
    print("=== 开始图片处理测试资源注册 ===\n")

    try:
        register_image_stages()
        register_deployments()
        register_applications()
        verify_resources()

        print("\n=== 资源注册完成 ===")
        print("\n下一步：")
        print("1. 上传一张测试图片")
        print("2. 提交任务处理图片")

    except Exception as e:
        print(f"\n注册过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
