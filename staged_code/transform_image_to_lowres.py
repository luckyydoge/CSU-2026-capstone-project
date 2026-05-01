import io
from PIL import Image


def run(data, config=None):
    """将 image 降低分辨率（低分辨率中间结果）"""
    file_content = data.get("file_content") if isinstance(data, dict) else data
    if not file_content:
        return data

    config = config or {}
    ratio = config.get("ratio", 0.25)

    image = Image.open(io.BytesIO(file_content))
    w, h = image.size
    new_size = (int(w * ratio), int(h * ratio))
    thumb = image.resize(new_size, Image.LANCZOS)

    buf = io.BytesIO()
    thumb.save(buf, format="PNG")

    return {
        "file_content": buf.getvalue(),
        "metadata": {
            "original_size": {"width": w, "height": h},
            "lowres_size": {"width": new_size[0], "height": new_size[1]},
            "ratio": ratio,
        },
    }
