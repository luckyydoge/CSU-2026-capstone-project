#!/usr/bin/env python3
"""
演示数据初始化脚本
用法: python scripts/init_demo.py
前置条件: 服务已运行在 http://127.0.0.1:8001
"""
import requests
import sys

BASE = "http://127.0.0.1:8001/api/v1"
ok = True

def api(method, path, json_data=None, label=""):
    global ok
    url = f"{BASE}{path}"
    try:
        if method == "POST":
            r = requests.post(url, json=json_data, timeout=10)
        elif method == "PUT":
            r = requests.put(url, json=json_data, timeout=10)
        elif method == "DELETE":
            r = requests.delete(url, timeout=10)
        else:
            r = requests.get(url, timeout=10)
        if r.status_code in (200, 201, 202, 204):
            return r.json() if r.status_code != 204 else None
        detail = r.json().get("detail", "") if r.headers.get("content-type", "").startswith("application/json") else ""
        if "already exists" in detail or "already exists" in r.text:
            print(f"  ⏭️  {label or path} (已存在)")
            return None
        print(f"  ❌ {method} {path} -> {r.status_code}: {detail or r.text[:80]}")
        ok = False
        return None
    except Exception as e:
        print(f"  ❌ {method} {path} -> {e}")
        ok = False
        return None


def main():
    print("=" * 60)
    print("协同推理平台 - 演示数据初始化")
    print("=" * 60)

    # ====== 1. 阶段 ======
    print("\n[1/6] 注册阶段...")
    for s in [
        {"name": "resize_10", "handler": "resize_10:run", "input_type": "image", "output_type": "image", "description": "图片分辨率降低10%"},
        {"name": "resize_20", "handler": "resize_20:run", "input_type": "image", "output_type": "image", "description": "图片分辨率降低20%"},
        {"name": "image_output", "handler": "image_output:run", "input_type": "image", "output_type": "image", "description": "图片输出保存"},
    ]:
        r = api("POST", "/stages", s, label=s["name"])
        if r:
            print(f"  ✅ {s['name']}")

    # ====== 2. 策略 ======
    print("\n[2/6] 注册策略...")
    for s in [
        {"name": "debug_end", "strategy_type": "routing", "handler": "debug_strategy:debug_strategy_end", "description": "强制调度到end节点"},
        {"name": "debug_edge", "strategy_type": "routing", "handler": "debug_strategy:debug_strategy_edge", "description": "强制调度到edge节点"},
        {"name": "debug_cloud", "strategy_type": "routing", "handler": "debug_strategy:debug_strategy_cloud", "description": "强制调度到cloud节点"},
        {"name": "random_routing", "strategy_type": "routing", "handler": "random_routing:decide", "description": "随机路由策略"},
        {"name": "resource_aware", "strategy_type": "routing", "handler": "resource_aware_routing:decide", "config": {"cpu_threshold": 80, "mem_threshold": 80}, "description": "依据节点 CPU/内存负载动态调度"},
    ]:
        r = api("POST", "/strategies", s, label=s["name"])
        if r:
            print(f"  ✅ {s['name']}")

    # ====== 3. 部署配置 ======
    print("\n[3/6] 注册部署配置...")
    for d in [
        {"stage_name": "resize_10", "allowed_tiers": ["end", "edge", "cloud"], "resources": {"cpu_cores": 0.5, "memory_mb": 512}},
        {"stage_name": "resize_20", "allowed_tiers": ["end", "edge", "cloud"], "resources": {"cpu_cores": 0.5, "memory_mb": 512}},
        {"stage_name": "image_output", "allowed_tiers": ["cloud"], "resources": {"cpu_cores": 0.2, "memory_mb": 256}},
    ]:
        r = api("POST", "/deployments", d, label=d["stage_name"])
        if r:
            print(f"  ✅ {d['stage_name']}")

    # ====== 4. 应用 ======
    print("\n[4/6] 注册应用...")
    r = api("POST", "/applications", {
        "name": "image_process_app",
        "description": "图片处理流水线: 缩小10% → 缩小20% → 输出",
        "input_type": "image",
        "stages": ["resize_10", "resize_20", "image_output"],
        "edges": [
            {"from_stage": "resize_10", "to_stage": "resize_20"},
            {"from_stage": "resize_20", "to_stage": "image_output"},
        ],
        "entry_stage": "resize_10",
        "exit_stages": ["image_output"],
    }, label="image_process_app")
    if r:
        print(f"  ✅ image_process_app")

    # ====== 5. 模拟节点 + 模型 + 数据变换 ======
    print("\n[5/6] 注册扩展数据...")

    # 注册模型
    r = api("POST", "/models", {
        "name": "resize_model", "version": "1.0", "stage_name": "resize_10",
        "load_method": "PIL.Image.open", "inference_config": {"format": "PNG"},
        "alternative_models": {"v0.5": {"quality": 50}, "v2.0": {"quality": 95}},
    }, label="resize_model")
    if r:
        print(f"  ✅ resize_model v1.0")

    # 注册数据变换
    for t in [
        {"name": "image_to_patches", "input_type": "image", "output_type": "patches", "handler": "transform_image_to_patches:run", "config": {"rows": 2, "cols": 2}},
        {"name": "image_to_lowres", "input_type": "image", "output_type": "lowres", "handler": "transform_image_to_lowres:run", "config": {"ratio": 0.25}},
        {"name": "extract_roi", "input_type": "image", "output_type": "roi", "handler": "transform_extract_roi:run", "config": {"crop_ratio": 0.6}},
    ]:
        r = api("POST", "/data-transforms", t, label=t["name"])
        if r:
            print(f"  ✅ {t['name']}")

    # ====== 6. 模拟节点 ======
    print("\n[6/6] 注册模拟节点（仅本地演示用）...")
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from app.database import SessionLocal
    from service.node_info_service import NodeInfoService
    import json

    nodes = [
        {"id": "node-end-1",    "host": "end-device-1",   "tier": "end",   "cpu": 2, "mem": 2048,  "cpu_pct": 85, "mem_pct": 90},
        {"id": "node-edge-1",   "host": "edge-server-1",  "tier": "edge",  "cpu": 8, "mem": 16384, "cpu_pct": 30, "mem_pct": 45},
        {"id": "node-edge-2",   "host": "edge-server-2",  "tier": "edge",  "cpu": 8, "mem": 16384, "cpu_pct": 95, "mem_pct": 88},
        {"id": "node-cloud-1",  "host": "cloud-node-1",   "tier": "cloud", "cpu": 32,"mem": 65536, "cpu_pct": 50, "mem_pct": 60},
    ]
    db = SessionLocal()
    for n in nodes:
        NodeInfoService._register_node_db(db,
            node_id=n["id"], hostname=n["host"], tier=n["tier"],
            cpu_cores=n["cpu"], memory_mb=n["mem"])
        NodeInfoService._update_load_db(db, n["id"], cpu_percent=n["cpu_pct"], memory_percent=n["mem_pct"])
        print(f"  ✅ {n['id']} ({n['tier']}) CPU={n['cpu_pct']}% MEM={n['mem_pct']}%")
    db.close()

    # ====== 汇总 ======
    print("\n" + "=" * 60)
    if ok:
        print("✅ 全部数据注册成功！")
        print("\n演示方式:")
        print("  1. 打开 http://127.0.0.1:8001/static/stage.html 查看阶段")
        print("  2. 打开 http://127.0.0.1:8001/static/strategy.html 查看策略(含 resource_aware)")
        print("  3. 打开 http://127.0.0.1:8001/static/node.html 查看模拟节点负载")
        print("  4. 提交任务测试: curl -X POST .../api/v1/tasks")
        print('     {"app_name":"image_process_app","strategy_name":"resource_aware","input_data":"demo"}')
    else:
        print("⚠️  部分数据注册失败")
        sys.exit(1)


if __name__ == "__main__":
    main()
