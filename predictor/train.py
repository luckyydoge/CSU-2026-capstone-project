"""
训练脚本：经典标样 + 随机数据 → 50轮训练 → 中位数聚合 → 保存系数 + 原始数据
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import random
import math
import csv
import json
from collections import defaultdict
from typing import List, Tuple
from predictor.models import LinearRegression, PredictorBundle

# ========== 经典标样 ==========

CLASSIC_IMAGES = [
    # (width, height, channels, file_size_kb, cpu, mem)  cpu/mem 为人工估计合理值
    (32, 32, 1, 1, 0.5, 5),
    (64, 64, 1, 3, 0.8, 8),
    (160, 120, 1, 8, 1.5, 12),
    (320, 240, 3, 20, 3.0, 20),
    (640, 480, 3, 60, 6.0, 35),
    (800, 600, 3, 100, 8.0, 50),
    (1280, 720, 3, 200, 15.0, 80),
    (1920, 1080, 3, 500, 35.0, 150),
    (2560, 1440, 3, 900, 55.0, 250),
    (3840, 2160, 3, 2000, 95.0, 420),
    (3840, 2160, 4, 2500, 105.0, 480),
    (7680, 4320, 3, 8000, 220.0, 950),
    (7680, 4320, 4, 10000, 260.0, 1100),
]

CLASSIC_VIDEOS = [
    # (duration_s, fps, width, height, bitrate_kbps, cpu, mem)
    (5, 15, 320, 240, 200, 8, 40),
    (10, 24, 640, 360, 500, 12, 55),
    (30, 30, 854, 480, 1000, 25, 80),
    (30, 30, 1280, 720, 2500, 40, 130),
    (60, 30, 1920, 1080, 5000, 80, 250),
    (120, 24, 1920, 1080, 8000, 150, 450),
    (60, 60, 3840, 2160, 30000, 250, 700),
    (600, 24, 1920, 1080, 8000, 500, 1200),
    (30, 30, 640, 360, 300, 10, 45),
    (10, 30, 3840, 2160, 50000, 180, 500),
]

CLASSIC_DATA = [
    # (record_count, field_count, total_size_kb, nesting_depth, cpu, mem)
    (1, 2, 0.5, 1, 0.5, 5),
    (10, 5, 1, 1, 1.0, 8),
    (50, 8, 5, 1, 2.0, 12),
    (100, 10, 10, 2, 3.5, 18),
    (500, 15, 30, 2, 6.0, 30),
    (1000, 20, 80, 2, 10.0, 50),
    (5000, 25, 200, 3, 18.0, 80),
    (10000, 30, 500, 3, 28.0, 130),
    (50000, 40, 2000, 4, 55.0, 300),
    (100000, 50, 5000, 4, 85.0, 550),
    (500000, 60, 10000, 5, 140.0, 1000),
]


# ========== 数据生成 ==========

def _gen_image_data(seed: int) -> Tuple[List[List[float]], List[float], List[float], List[dict]]:
    """生成图片数据：经典标样 + 随机扩展"""
    rng = random.Random(seed)
    X, y_cpu, y_mem, raw = [], [], [], []

    # 经典标样（精确覆盖）
    for item in CLASSIC_IMAGES:
        w, h, ch, sz, cpu, mem = item
        pixels_m = round(w * h / 1_000_000, 4)
        noise_cpu = cpu * rng.uniform(-0.05, 0.05)
        noise_mem = mem * rng.uniform(-0.05, 0.05)
        X.append([pixels_m, float(sz), float(ch)])
        y_cpu.append(max(0.5, cpu + noise_cpu))
        y_mem.append(max(5, mem + noise_mem))
        raw.append({"type": "image", "width": w, "height": h, "channels": ch,
                     "file_size_kb": sz, "cpu_true": cpu, "mem_true": mem})

    # 随机扩展（覆盖空白区域）
    for _ in range(1500):
        w = rng.randint(16, 7680)
        h = int(w * rng.uniform(0.25, 2.0))
        ch = rng.choice([1, 3, 4])
        sz = round(w * h * ch * rng.uniform(0.01, 0.5) / 1024, 1)
        pixels_m = round(w * h / 1_000_000, 4)

        cpu_base = 0.5 + pixels_m * 50 + sz * 0.01
        mem_base = 5 + pixels_m * 100 + sz * 0.05
        cpu = cpu_base + rng.gauss(0, max(1, cpu_base * 0.12))
        mem = mem_base + rng.gauss(0, max(2, mem_base * 0.10))

        X.append([pixels_m, sz, float(ch)])
        y_cpu.append(max(0.5, cpu))
        y_mem.append(max(5, mem))
        raw.append({"type": "image", "width": w, "height": h, "channels": ch,
                     "file_size_kb": sz, "cpu_true": round(cpu, 2), "mem_true": round(mem, 2)})

    return X, y_cpu, y_mem, raw


def _gen_video_data(seed: int) -> Tuple[List[List[float]], List[float], List[float], List[dict]]:
    rng = random.Random(seed)
    X, y_cpu, y_mem, raw = [], [], [], []

    for item in CLASSIC_VIDEOS:
        dur, fps, w, h, br, cpu, mem = item
        total_frames = dur * fps
        pixels_pp_m = round(w * h / 1_000_000, 4)
        noise_cpu = cpu * rng.uniform(-0.05, 0.05)
        noise_mem = mem * rng.uniform(-0.05, 0.05)
        X.append([dur, total_frames, pixels_pp_m, float(br)])
        y_cpu.append(max(1, cpu + noise_cpu))
        y_mem.append(max(10, mem + noise_mem))
        raw.append({"type": "video", "duration_s": dur, "fps": fps,
                     "width": w, "height": h, "total_frames": total_frames,
                     "pixels_per_frame_M": pixels_pp_m, "bitrate_kbps": br,
                     "cpu_true": cpu, "mem_true": mem})

    for _ in range(1500):
        dur = rng.uniform(1, 600)
        fps = rng.choice([15, 24, 25, 30, 60])
        w = rng.choice([160, 320, 640, 854, 1280, 1920, 3840, 7680])
        h = int(w * rng.uniform(0.5, 1.0))
        br = rng.uniform(100, 50000)
        total_frames = dur * fps
        pixels_pp_m = round(w * h / 1_000_000, 4)

        cpu_base = 2 + dur * 2 + pixels_pp_m * 25 * (fps / 30)
        mem_base = 20 + pixels_pp_m * 70 + dur * 5
        cpu = cpu_base + rng.gauss(0, max(2, cpu_base * 0.12))
        mem = mem_base + rng.gauss(0, max(5, mem_base * 0.10))

        X.append([dur, total_frames, pixels_pp_m, br])
        y_cpu.append(max(1, cpu))
        y_mem.append(max(10, mem))
        raw.append({"type": "video", "duration_s": round(dur, 1), "fps": fps,
                     "width": w, "height": h, "total_frames": total_frames,
                     "pixels_per_frame_M": pixels_pp_m, "bitrate_kbps": round(br, 0),
                     "cpu_true": round(cpu, 2), "mem_true": round(mem, 2)})

    return X, y_cpu, y_mem, raw


def _gen_data_data(seed: int) -> Tuple[List[List[float]], List[float], List[float], List[dict]]:
    rng = random.Random(seed)
    X, y_cpu, y_mem, raw = [], [], [], []

    for item in CLASSIC_DATA:
        rc, fc, sz, nd, cpu, mem = item
        noise_cpu = cpu * rng.uniform(-0.05, 0.05)
        noise_mem = mem * rng.uniform(-0.05, 0.05)
        X.append([float(rc), float(fc), float(sz), float(nd)])
        y_cpu.append(max(0.5, cpu + noise_cpu))
        y_mem.append(max(5, mem + noise_mem))
        raw.append({"type": "data", "record_count": rc, "field_count": fc,
                     "total_size_kb": sz, "nesting_depth": nd,
                     "cpu_true": cpu, "mem_true": mem})

    for _ in range(1500):
        rc = rng.randint(1, 1000000)
        fc = rng.randint(1, 100)
        sz = round(rc * fc * rng.uniform(0.001, 0.2), 1)
        nd = rng.randint(1, 8)

        cpu_base = 0.5 + math.log2(rc + 1) * 4 + fc * 0.5 + nd * 5
        mem_base = 5 + sz * 0.6 + fc * 2 + nd * 12
        cpu = cpu_base + rng.gauss(0, max(1, cpu_base * 0.15))
        mem = mem_base + rng.gauss(0, max(2, mem_base * 0.12))

        X.append([float(rc), float(fc), sz, float(nd)])
        y_cpu.append(max(0.5, cpu))
        y_mem.append(max(5, mem))
        raw.append({"type": "data", "record_count": rc, "field_count": fc,
                     "total_size_kb": sz, "nesting_depth": nd,
                     "cpu_true": round(cpu, 2), "mem_true": round(mem, 2)})

    return X, y_cpu, y_mem, raw


# ========== 工具 ==========

def _median(vals: List[float]) -> float:
    s = sorted(vals)
    n = len(s)
    if n == 0:
        return 0.0
    return s[n // 2] if n % 2 == 1 else (s[n // 2 - 1] + s[n // 2]) / 2.0


# ========== 主训练 ==========

def train(rounds: int = 50, output_dir: str = None):
    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(__file__), "coefficients")
    os.makedirs(output_dir, exist_ok=True)

    train_data_dir = os.path.join(os.path.dirname(__file__), "training_data")
    os.makedirs(train_data_dir, exist_ok=True)

    generators = [
        ("image", _gen_image_data, ["width", "height", "channels", "file_size_kb"]),
        ("video", _gen_video_data, ["duration_s", "fps", "width", "height", "bitrate_kbps"]),
        ("data", _gen_data_data, ["record_count", "field_count", "total_size_kb", "nesting_depth"]),
    ]

    print("=" * 70)
    print(f"训练资源预测模型 — {rounds} 轮中位数聚合")
    print("=" * 70)

    # 用于收集每轮的系数
    all_coefs = {}  # {dtype_target: [(intercept, [coef...])]}

    for dtype, gen_fn, feat_names in generators:
        dtype_label = {"image": "图片", "video": "视频", "data": "JSON数据"}[dtype]
        print(f"\n{'─' * 70}")
        print(f"[{dtype_label}] 开始训练...")
        all_X, all_raw_data = None, None

        for r in range(1, rounds + 1):
            seed = r * 100 + hash(dtype) % 10000
            X, y_cpu, y_mem, raw_data = gen_fn(seed)

            # 第一轮保存原始数据
            if all_X is None:
                all_X = X
                all_raw_data = raw_data

            # 训练 CPU 模型
            m_cpu = LinearRegression()
            m_cpu.fit(X, y_cpu)

            # 训练 MEM 模型
            m_mem = LinearRegression()
            m_mem.fit(X, y_mem)

            # 收集系数
            for target, model in [("cpu", m_cpu), ("mem", m_mem)]:
                key = f"{dtype}_{target}"
                all_coefs.setdefault(key, []).append((model.intercept_, model.coef_[:]))

            if r % 10 == 0:
                print(f"  轮次 {r}/{rounds} — {dtype_label} 训练完毕")

        # 保存原始训练数据 (CSV)
        csv_path = os.path.join(train_data_dir, f"{dtype}_training_data.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            if raw_data:
                writer = csv.DictWriter(f, fieldnames=raw_data[0].keys())
                writer.writeheader()
                writer.writerows(raw_data)
        print(f"  ✅ 训练数据已保存: {csv_path} ({len(raw_data)} 条)")

        # 保存单轮特征向量（X, y_cpu, y_mem）为 JSON
        feat_path = os.path.join(train_data_dir, f"{dtype}_features.json")
        with open(feat_path, "w") as f:
            json.dump({
                "feature_names": feat_names,
                "X": [[round(v, 4) for v in row] for row in all_X],
                "y_cpu": [round(v, 4) for v in y_cpu],
                "y_mem": [round(v, 4) for v in y_mem],
            }, f, indent=1)
        print(f"  ✅ 特征向量已保存: {feat_path}")

    # ====== 中位数聚合 ======
    print(f"\n{'=' * 70}")
    print("中位数聚合系数")
    print(f"{'=' * 70}")

    bundle = PredictorBundle()
    for key, coef_list in all_coefs.items():
        dtype, target = key.split("_")
        # 收集 intercept 列和各 coef 列
        intercepts = [c[0] for c in coef_list]
        n_coefs = len(coef_list[0][1])
        coef_cols = [[c[1][i] for c in coef_list] for i in range(n_coefs)]

        median_intercept = _median(intercepts)
        median_coefs = [_median(col) for col in coef_cols]

        # 写入 bundle 模型
        model = LinearRegression()
        model.intercept_ = median_intercept
        model.coef_ = median_coefs
        bundle.models[dtype][target] = model

        # 打印方程
        terms = [f"{c:.6f}*x{i+1}" for i, c in enumerate(median_coefs)]
        equation = f"{median_intercept:.4f} + " + " + ".join(terms)
        print(f"\n[{dtype}.{target}]")
        print(f"  y = {equation}")

    # 保存最终系数
    bundle.save_all(output_dir)
    print(f"\n✅ 最终系数已保存到 {output_dir}/")
    print(f"  文件列表: {os.listdir(output_dir)}")

    # 打印使用说明
    print(f"\n{'=' * 70}")
    print("训练数据文件说明")
    print(f"{'=' * 70}")
    for dtype in ["image", "video", "data"]:
        csv_p = os.path.join(train_data_dir, f"{dtype}_training_data.csv")
        json_p = os.path.join(train_data_dir, f"{dtype}_features.json")
        print(f"  {dtype}:")
        print(f"    CSV: {csv_p}  (含真实 CPU/MEM 值)")
        print(f"    JSON: {json_p}  (特征向量 + CPU/MEM 标签)")

    print(f"\n{'=' * 70}")
    print("使用训练好的模型进行预测")
    print(f"{'=' * 70}")
    print("  from predictor.predict import predict")
    print('  result = predict("image", {"width": 1920, "height": 1080, "channels": 3, "file_size_kb": 500})')
    print("  # → {'cpu_percent': ..., 'memory_mb': ...}")


if __name__ == "__main__":
    train(rounds=50)
