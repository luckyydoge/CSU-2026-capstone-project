"""
预测入口：加载训练好的系数，对输入数据进行资源预测
"""
import os
from typing import Dict, Any, Optional
from predictor.feature_extract import extract, FEATURE_NAMES
from predictor.models import PredictorBundle, LinearRegression


_bundle: Optional[PredictorBundle] = None


def _get_bundle() -> PredictorBundle:
    global _bundle
    if _bundle is None:
        coeff_dir = os.path.join(os.path.dirname(__file__), "coefficients")
        _bundle = PredictorBundle.load_all(coeff_dir, predictor_cls=LinearRegression)
    return _bundle


def predict(data_type: str, raw_data: Dict[str, Any]) -> Dict[str, float]:
    """
    预测输入数据所需的资源

    参数:
        data_type: "image" | "video" | "data"
        raw_data: 包含特征字段的 dict

    返回:
        {"cpu_percent": float, "memory_mb": float}
    """
    bundle = _get_bundle()
    features = extract(data_type, raw_data)
    return bundle.predict(data_type, features)


def describe_features(data_type: str) -> str:
    """返回特征说明，供调用方了解需要提供哪些字段"""
    names = FEATURE_NAMES.get(data_type, [])
    return f"请提供: {', '.join(names)}"


def predict_raw(data_type: str, features: list) -> Dict[str, float]:
    """直接用特征向量预测（跳过特征提取）"""
    bundle = _get_bundle()
    return bundle.predict(data_type, features)


def reload():
    """重新加载系数（训练新模型后调用）"""
    global _bundle
    _bundle = None
