#!/usr/bin/env python3
"""
文件上传和处理测试脚本
测试文件上传、下载和删除功能
"""

import requests
import json
import os
import time

BASE_URL = "http://127.0.0.1:8001/api/v1"

def test_file_upload():
    """测试文件上传"""
    print("=== 测试文件上传 ===")

    test_files = [
        {
            "filename": "test_image.png",
            "content": b"\x89PNG\r\n\x1a\n" + b"\x00" * 100,
            "description": "PNG图片文件"
        },
        {
            "filename": "test_video.mp4",
            "content": b"\x00\x00\x00\x18ftypmp4" + b"\x00" * 100,
            "description": "MP4视频文件"
        },
        {
            "filename": "test_document.txt",
            "content": b"Hello, this is a test document with some content.",
            "description": "文本文件"
        },
        {
            "filename": "test_large_file.bin",
            "content": b"\x00" * 1024 * 100,
            "description": "大文件（100KB）"
        }
    ]

    uploaded_files = []

    for test_file in test_files:
        print(f"\n上传文件: {test_file['filename']} ({test_file['description']})")

        try:
            files = {"file": (test_file["filename"], test_file["content"])}
            response = requests.post(f"{BASE_URL}/files/upload", files=files, timeout=10)

            print(f"  状态码: {response.status_code}")
            print(f"  响应: {json.dumps(response.json(), indent=2)}")

            if response.status_code == 201:
                file_info = response.json()
                uploaded_files.append(file_info)
                print(f"  ✅ 上传成功: file_id={file_info['file_id']}")
            else:
                print(f"  ❌ 上传失败")

        except Exception as e:
            print(f"  ❌ 上传出错: {str(e)}")

    return uploaded_files

def test_file_list(uploaded_files):
    """测试文件列表"""
    print("\n=== 测试文件列表 ===")

    try:
        response = requests.get(f"{BASE_URL}/files", timeout=10)
        print(f"状态码: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print(f"文件总数: {result['total']}")
            print(f"文件列表:")
            for file_info in result["files"]:
                print(f"  - {file_info['original_filename']}: {file_info['size']} bytes")
            print("  ✅ 文件列表获取成功")
        else:
            print(f"  ❌ 获取文件列表失败")

    except Exception as e:
        print(f"  ❌ 获取文件列表出错: {str(e)}")

def test_file_info(uploaded_files):
    """测试获取文件信息"""
    print("\n=== 测试获取文件信息 ===")

    if not uploaded_files:
        print("没有已上传的文件，跳过测试")
        return

    file_id = uploaded_files[0]["file_id"]
    print(f"获取文件信息: file_id={file_id}")

    try:
        response = requests.get(f"{BASE_URL}/files/{file_id}", timeout=10)
        print(f"状态码: {response.status_code}")

        if response.status_code == 200:
            file_info = response.json()
            print(f"文件信息:")
            print(f"  - 文件ID: {file_info['file_id']}")
            print(f"  - 原文件名: {file_info['original_filename']}")
            print(f"  - 文件大小: {file_info['size']} bytes")
            print(f"  - 上传时间: {file_info['upload_time']}")
            print("  ✅ 获取文件信息成功")
        else:
            print(f"  ❌ 获取文件信息失败")

    except Exception as e:
        print(f"  ❌ 获取文件信息出错: {str(e)}")

def test_file_download(uploaded_files):
    """测试文件下载"""
    print("\n=== 测试文件下载 ===")

    if not uploaded_files:
        print("没有已上传的文件，跳过测试")
        return

    file_id = uploaded_files[0]["file_id"]
    filename = uploaded_files[0]["original_filename"]
    original_size = uploaded_files[0]["size"]

    print(f"下载文件: file_id={file_id}, filename={filename}")

    try:
        response = requests.get(f"{BASE_URL}/files/{file_id}/download", timeout=10)
        print(f"状态码: {response.status_code}")

        if response.status_code == 200:
            downloaded_content = response.content
            print(f"下载内容大小: {len(downloaded_content)} bytes")
            print(f"原始文件大小: {original_size} bytes")

            if len(downloaded_content) == original_size:
                print("  ✅ 文件下载成功，内容大小匹配")
            else:
                print("  ⚠️ 文件下载成功，但内容大小不匹配")
        else:
            print(f"  ❌ 文件下载失败")

    except Exception as e:
        print(f"  ❌ 文件下载出错: {str(e)}")

def test_file_delete(uploaded_files):
    """测试文件删除"""
    print("\n=== 测试文件删除 ===")

    if not uploaded_files:
        print("没有已上传的文件，跳过测试")
        return

    file_id = uploaded_files[-1]["file_id"]
    filename = uploaded_files[-1]["original_filename"]

    print(f"删除文件: file_id={file_id}, filename={filename}")

    try:
        response = requests.delete(f"{BASE_URL}/files/{file_id}", timeout=10)
        print(f"状态码: {response.status_code}")

        if response.status_code == 200:
            print("  ✅ 文件删除成功")
            uploaded_files.pop()
        else:
            print(f"  ❌ 文件删除失败")

    except Exception as e:
        print(f"  ❌ 文件删除出错: {str(e)}")

def test_file_with_task(uploaded_files):
    """测试使用文件ID提交任务"""
    print("\n=== 测试使用文件ID提交任务 ===")

    if not uploaded_files:
        print("没有已上传的文件，跳过测试")
        return

    file_id = uploaded_files[0]["file_id"]

    print(f"提交任务，使用文件ID: {file_id}")

    task_data = {
        "application_name": "basic_app",
        "strategy_name": "debug_end",
        "input_data": {
            "file_id": file_id,
            "metadata": {
                "filename": uploaded_files[0]["original_filename"],
                "size": uploaded_files[0]["size"]
            }
        },
        "runtime_config": {}
    }

    try:
        response = requests.post(f"{BASE_URL}/tasks", json=task_data, timeout=30)
        print(f"状态码: {response.status_code}")
        print(f"响应: {json.dumps(response.json(), indent=2)}")

        if response.status_code == 202:
            task_id = response.json().get("task_id")
            print(f"  ✅ 任务提交成功: task_id={task_id}")

            # 等待任务完成
            print("等待任务完成...")
            for i in range(10):
                time.sleep(2)
                response = requests.get(f"{BASE_URL}/tasks/{task_id}", timeout=10)
                if response.status_code == 200:
                    task_info = response.json()
                    status = task_info.get("status")
                    print(f"任务状态: {status}")

                    if status == "completed":
                        print("  ✅ 任务执行成功")
                        break
                    elif status == "failed":
                        print("  ❌ 任务执行失败")
                        break
        else:
            print(f"  ❌ 任务提交失败")

    except Exception as e:
        print(f"  ❌ 任务提交出错: {str(e)}")

def main():
    """主测试函数"""
    print("=== 开始文件上传和处理测试 ===\n")

    print("=== 测试配置 ===")
    print(f"服务地址: {BASE_URL}")
    print()

    uploaded_files = []

    try:
        # 1. 测试文件上传
        uploaded_files = test_file_upload()

        # 2. 测试文件列表
        test_file_list(uploaded_files)

        # 3. 测试获取文件信息
        test_file_info(uploaded_files)

        # 4. 测试文件下载
        test_file_download(uploaded_files)

        # 5. 测试使用文件ID提交任务
        test_file_with_task(uploaded_files)

        # 6. 测试文件删除
        test_file_delete(uploaded_files)

        print("\n=== 测试完成 ===")

        if uploaded_files:
            print(f"\n剩余文件数: {len(uploaded_files)}")
            for file_info in uploaded_files:
                print(f"  - {file_info['original_filename']}: {file_info['size']} bytes")

    except Exception as e:
        print(f"\n测试过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
