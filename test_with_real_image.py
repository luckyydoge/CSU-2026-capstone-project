#!/usr/bin/env python3
"""
使用真实图片测试图片处理流程
"""

import requests
import os

BASE_URL = "http://127.0.0.1:8001/api/v1"

def upload_real_image():
    """上传真实图片"""
    print("=== 上传真实图片 ===")

    image_path = input("请输入图片路径: ").strip()

    if not os.path.exists(image_path):
        print(f"文件不存在: {image_path}")
        return None

    try:
        with open(image_path, "rb") as f:
            files = {"file": (os.path.basename(image_path), f, "image/png")}
            response = requests.post(f"{BASE_URL}/files/upload", files=files, timeout=30)

        if response.status_code == 201:
            file_info = response.json()
            print(f"✅ 上传成功!")
            print(f"  file_id: {file_info['file_id']}")
            print(f"  文件名: {file_info['original_filename']}")
            print(f"  文件大小: {file_info['size']} bytes")
            return file_info
        else:
            print(f"❌ 上传失败: {response.text}")
            return None

    except Exception as e:
        print(f"❌ 上传出错: {str(e)}")
        return None

def submit_task(file_id):
    """提交任务"""
    print("\n=== 提交图片处理任务 ===")

    task_data = {
        "application_name": "image_process_app",
        "strategy_name": "debug_edge",
        "input_data": {
            "file_id": file_id,
            "metadata": {"format": "PNG"}
        },
        "runtime_config": {}
    }

    try:
        response = requests.post(f"{BASE_URL}/tasks", json=task_data, timeout=60)
        if response.status_code == 202:
            task_id = response.json().get("task_id")
            print(f"✅ 任务提交成功!")
            print(f"  task_id: {task_id}")
            return task_id
        else:
            print(f"❌ 任务提交失败: {response.text}")
            return None
    except Exception as e:
        print(f"❌ 任务提交出错: {str(e)}")
        return None

def check_task(task_id):
    """检查任务"""
    import time

    print("\n=== 检查任务状态 ===")

    for i in range(20):
        try:
            response = requests.get(f"{BASE_URL}/tasks/{task_id}", timeout=10)
            if response.status_code == 200:
                task_info = response.json()
                status = task_info.get("status")
                print(f"状态: {status} (等待 {i*2}s)")

                if status == "completed":
                    trace = task_info.get("trace", {})
                    print("\n执行路径:")
                    for step in trace.get("execution_path", []):
                        print(f"  {step.get('stage_name')}: {step.get('node_tier')} - {step.get('execution_time_ms', 0):.2f}ms")

                    final = task_info.get("final_output", {})
                    print(f"\n最终输出: {final}")
                    return True
                elif status == "failed":
                    print("❌ 任务失败")
                    return False
        except Exception as e:
            print(f"检查出错: {str(e)}")

        time.sleep(2)

    print("⚠️ 超时")
    return False

def main():
    print("=" * 60)
    print("图片处理测试 - 使用真实图片")
    print("=" * 60)

    file_info = upload_real_image()
    if not file_info:
        return

    task_id = submit_task(file_info["file_id"])
    if not task_id:
        return

    check_task(task_id)

if __name__ == "__main__":
    main()
