#!/usr/bin/env python3
"""
图片处理完整测试脚本
上传图片 -> 提交任务 -> 执行resize_10 -> resize_20 -> output
"""

import requests
import json
import os

BASE_URL = "http://127.0.0.1:8001/api/v1"

def create_test_image():
    """创建一个测试图片"""
    print("=== 创建测试图片 ===")

    try:
        from PIL import Image
        import io

        width, height = 800, 600
        image = Image.new('RGB', (width, height), color='red')

        img_bytes = io.BytesIO()
        image.save(img_bytes, format='PNG')
        img_bytes.seek(0)

        print(f"创建测试图片: {width}x{height}")
        return img_bytes.getvalue()

    except ImportError:
        print("PIL not installed, creating dummy image data")
        return b"\x89PNG\r\n\x1a\n" + b"\x00" * 1000

def upload_test_image():
    """上传测试图片"""
    print("\n=== 上传测试图片 ===")

    test_image = create_test_image()

    try:
        files = {"file": ("test_image.png", test_image, "image/png")}
        response = requests.post(f"{BASE_URL}/files/upload", files=files, timeout=10)

        print(f"上传状态: {response.status_code}")

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

def submit_image_task(file_id):
    """提交图片处理任务"""
    print("\n=== 提交图片处理任务 ===")

    task_data = {
        "application_name": "image_process_app",
        "strategy_name": "debug_edge",
        "input_data": {
            "file_id": file_id,
            "metadata": {
                "format": "PNG",
                "source": "test_image"
            }
        },
        "runtime_config": {}
    }

    try:
        print("提交任务...")
        print(f"  应用: {task_data['application_name']}")
        print(f"  策略: {task_data['strategy_name']}")
        print(f"  file_id: {file_id}")

        response = requests.post(f"{BASE_URL}/tasks", json=task_data, timeout=60)
        print(f"\n任务提交状态: {response.status_code}")

        if response.status_code == 202:
            result = response.json()
            task_id = result.get("task_id")
            print(f"✅ 任务提交成功!")
            print(f"  task_id: {task_id}")
            return task_id
        else:
            print(f"❌ 任务提交失败: {response.text}")
            return None

    except Exception as e:
        print(f"❌ 任务提交出错: {str(e)}")
        return None

def wait_and_check_task(task_id):
    """等待任务完成并检查结果"""
    print("\n=== 检查任务执行结果 ===")

    import time

    max_wait = 60
    interval = 3
    elapsed = 0

    while elapsed < max_wait:
        try:
            response = requests.get(f"{BASE_URL}/tasks/{task_id}", timeout=10)
            if response.status_code == 200:
                task_info = response.json()
                status = task_info.get("status")
                print(f"任务状态: {status} (等待 {elapsed}s)")

                if status == "completed":
                    print("\n✅ 任务执行成功!")

                    trace = task_info.get("trace", {})
                    execution_path = trace.get("execution_path", [])

                    print("\n执行路径:")
                    for step in execution_path:
                        print(f"  阶段: {step.get('stage_name')}")
                        print(f"  节点层级: {step.get('node_tier')}")
                        print(f"  执行时间: {step.get('execution_time_ms', 0):.2f}ms")
                        print()

                    final_output = task_info.get("final_output", {})
                    print("最终输出:")
                    print(f"  状态: {final_output.get('status')}")
                    print(f"  输出路径: {final_output.get('output_path')}")
                    print(f"  输出大小: {final_output.get('output_size')} bytes")
                    print(f"  最终尺寸: {final_output.get('final_size')}")

                    return True
                elif status == "failed":
                    print("\n❌ 任务执行失败")
                    error_logs = trace.get("error_logs", [])
                    for error in error_logs:
                        print(f"  错误: {error}")
                    return False

            time.sleep(interval)
            elapsed += interval

        except Exception as e:
            print(f"检查任务状态出错: {str(e)}")
            time.sleep(interval)
            elapsed += interval

    print("\n⚠️ 任务执行超时")
    return False

def main():
    """主函数"""
    print("=" * 60)
    print("图片处理完整测试")
    print("流程: 上传图片 -> resize_10(缩小10%) -> resize_20(缩小20%) -> 输出")
    print("=" * 60)

    try:
        # 1. 上传测试图片
        file_info = upload_test_image()
        if not file_info:
            print("\n❌ 图片上传失败，退出测试")
            return

        file_id = file_info["file_id"]

        # 2. 提交图片处理任务
        task_id = submit_image_task(file_id)
        if not task_id:
            print("\n❌ 任务提交失败，退出测试")
            return

        # 3. 等待并检查任务结果
        success = wait_and_check_task(task_id)

        print("\n" + "=" * 60)
        if success:
            print("✅ 测试完成!")
        else:
            print("❌ 测试失败!")
        print("=" * 60)

    except Exception as e:
        print(f"\n测试过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
