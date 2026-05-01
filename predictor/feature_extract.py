"""
特征提取：将输入数据转换为特征向量
"""
from typing import Dict, List, Any


def extract_image_features(data: Dict[str, Any]) -> List[float]:
    """提取图片特征: [total_pixels_M, file_size_kb, channels]"""
    w = float(data.get("width", 640))
    h = float(data.get("height", 480))
    ch = float(data.get("channels", 3))
    sz = float(data.get("file_size_kb", 100))
    pixels_m = w * h / 1_000_000  # 百万像素
    return [pixels_m, sz, ch]


def extract_video_features(data: Dict[str, Any]) -> List[float]:
    """提取视频特征: [duration_s, total_frames, pixels_per_frame_M, bitrate_kbps]"""
    dur = float(data.get("duration_s", 10))
    fps = float(data.get("fps", 30))
    w = float(data.get("width", 1280))
    h = float(data.get("height", 720))
    br = float(data.get("bitrate_kbps", 2000))
    total_frames = dur * fps
    pixels_per_frame_m = w * h / 1_000_000
    return [dur, total_frames, pixels_per_frame_m, br]


def extract_data_features(data: Dict[str, Any]) -> List[float]:
    """提取数据特征: [record_count, field_count, total_size_kb, nesting_depth]"""
    rc = float(data.get("record_count", 100))
    fc = float(data.get("field_count", 10))
    sz = float(data.get("total_size_kb", 50))
    nd = float(data.get("nesting_depth", 1))
    return [rc, fc, sz, nd]


EXTRACTORS = {
    "image": extract_image_features,
    "video": extract_video_features,
    "data": extract_data_features,
}


def extract(data_type: str, raw: Dict[str, Any]) -> List[float]:
    fn = EXTRACTORS.get(data_type)
    if not fn:
        raise ValueError(f"Unknown data type: {data_type}, supported: {list(EXTRACTORS.keys())}")
    return fn(raw)


FEATURE_NAMES = {
    "image": ["total_pixels_M", "file_size_kb", "channels"],
    "video": ["duration_s", "total_frames", "pixels_per_frame_M", "bitrate_kbps"],
    "data": ["record_count", "field_count", "total_size_kb", "nesting_depth"],
}


def describe(data_type: str) -> str:
    names = FEATURE_NAMES.get(data_type, [])
    return f"{data_type}: {', '.join(names)}"
