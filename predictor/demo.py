#!/usr/bin/env python3
"""
演示脚本：训练 → 预测 → 展示结果
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from predictor.train import train
from predictor.predict import predict


def main():
    # 1. 训练模型
    train()

    print("\n" + "=" * 60)
    print("预测演示")
    print("=" * 60)

    cases = [
        ("image", "小图片 (640x480)", {"width": 640, "height": 480, "channels": 3, "file_size_kb": 100}),
        ("image", "大图片 (3840x2160)", {"width": 3840, "height": 2160, "channels": 3, "file_size_kb": 1500}),
        ("video", "短视频 (30s 1080p)", {"duration_s": 30, "fps": 30, "width": 1920, "height": 1080, "bitrate_kbps": 5000}),
        ("video", "长视频 (120s 4K)", {"duration_s": 120, "fps": 60, "width": 3840, "height": 2160, "bitrate_kbps": 20000}),
        ("data", "小数据 (100条)", {"record_count": 100, "field_count": 10, "total_size_kb": 50, "nesting_depth": 1}),
        ("data", "大数据 (50000条)", {"record_count": 50000, "field_count": 50, "total_size_kb": 5000, "nesting_depth": 5}),
    ]

    for dtype, label, raw in cases:
        result = predict(dtype, raw)
        print(f"\n[{dtype}] {label}")
        print(f"  特征: {raw}")
        print(f"  预测: CPU={result['cpu_percent']}%  MEM={result['memory_mb']}MB")


if __name__ == "__main__":
    main()
