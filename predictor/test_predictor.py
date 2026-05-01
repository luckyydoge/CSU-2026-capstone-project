#!/usr/bin/env python3
"""
单元测试
"""
import sys
import os
import json
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from predictor.models import LinearRegression, PredictorBundle
from predictor.feature_extract import extract, EXTRACTORS, describe
from predictor.predict import predict, predict_raw, reload


def test_linear_regression_fit_predict():
    """测试线性回归拟合和预测"""
    # y = 2 + 3*x1 + 0.5*x2
    X = [[1.0, 2.0], [2.0, 3.0], [3.0, 4.0], [4.0, 5.0], [5.0, 6.0]]
    y = [2 + 3 * x[0] + 0.5 * x[1] for x in X]
    model = LinearRegression()
    model.fit(X, y)
    pred = model.predict([[6.0, 7.0]])[0]
    expected = 2 + 3 * 6.0 + 0.5 * 7.0
    assert abs(pred - expected) < 1e-6, f"{pred} != {expected}"
    print("✅ test_linear_regression_fit_predict")


def test_save_load():
    """测试保存和加载"""
    model = LinearRegression()
    model.intercept_ = 1.5
    model.coef_ = [2.0, 3.0]
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        path = f.name
        model.save(path)
    loaded = LinearRegression.load(path)
    assert loaded.intercept_ == 1.5
    assert loaded.coef_ == [2.0, 3.0]
    os.unlink(path)
    print("✅ test_save_load")


def test_bundle():
    """测试 PredictorBundle 训练 + 预测 + 保存 + 加载"""
    bundle = PredictorBundle()
    # 3 features to match image: [pixels_M, file_size_kb, channels]
    X = [[1.0, 50.0, 3.0], [3.0, 100.0, 3.0]]
    y_cpu = [10.0, 20.0]
    y_mem = [100.0, 200.0]
    bundle.fit("image", X, y_cpu, y_mem)

    result = bundle.predict("image", [2.0, 80.0, 3.0])
    assert "cpu_percent" in result
    assert "memory_mb" in result
    assert result["cpu_percent"] >= 0
    print("✅ test_bundle")


def test_feature_extract():
    """测试特征提取"""
    image_features = extract("image", {"width": 1920, "height": 1080, "channels": 3, "file_size_kb": 500})
    assert len(image_features) == 3
    # pixels = 1920*1080/1e6 ≈ 2.0736
    assert abs(image_features[0] - 2.0736) < 0.01
    assert image_features[1] == 500
    assert image_features[2] == 3

    video_features = extract("video", {"duration_s": 30, "fps": 30, "width": 1920, "height": 1080, "bitrate_kbps": 5000})
    assert len(video_features) == 4
    # total_frames = 30*30 = 900
    assert video_features[1] == 900

    data_features = extract("data", {"record_count": 100, "field_count": 10, "total_size_kb": 50, "nesting_depth": 2})
    assert len(data_features) == 4

    print("✅ test_feature_extract")


def test_predict():
    """测试完整预测流程"""
    reload()
    result = predict("image", {"width": 640, "height": 480, "channels": 3, "file_size_kb": 100})
    assert "cpu_percent" in result
    assert "memory_mb" in result
    assert result["cpu_percent"] >= 0
    print(f"  predict(image, 640x480) → CPU={result['cpu_percent']}% MEM={result['memory_mb']}MB")
    print("✅ test_predict")


def test_predict_raw():
    """测试直接用特征向量预测"""
    reload()
    result = predict_raw("image", [1920, 1080, 3, 500])
    assert "cpu_percent" in result
    assert result["cpu_percent"] >= 0
    print(f"  predict_raw(image, ...) → CPU={result['cpu_percent']}% MEM={result['memory_mb']}MB")
    print("✅ test_predict_raw")


if __name__ == "__main__":
    # 先训练模型
    from predictor.train import train
    train()

    print("\n" + "=" * 60)
    print("运行测试")
    print("=" * 60)
    test_linear_regression_fit_predict()
    test_save_load()
    test_bundle()
    test_feature_extract()
    test_predict()
    test_predict_raw()
    print("\n✅ 全部测试通过")
