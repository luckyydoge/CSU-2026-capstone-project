import io
from PIL import Image


def run(data, config=None):
    """从 image 中提取 ROI（居中裁剪）"""
    file_content = data.get("file_content") if isinstance(data, dict) else data
    if not file_content:
        return data

    config = config or {}
    crop_ratio = config.get("crop_ratio", 0.6)

    image = Image.open(io.BytesIO(file_content))
    w, h = image.size
    cw, ch = int(w * crop_ratio), int(h * crop_ratio)
    left = (w - cw) // 2
    top = (h - ch) // 2
    roi = image.crop((left, top, left + cw, top + ch))

    buf = io.BytesIO()
    roi.save(buf, format="PNG")

    return {
        "file_content": buf.getvalue(),
        "roi_box": {"x": left, "y": top, "width": cw, "height": ch},
        "metadata": {"original_size": {"width": w, "height": h}, "crop_ratio": crop_ratio},
    }
