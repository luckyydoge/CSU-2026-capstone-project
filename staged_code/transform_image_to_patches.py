import io
from PIL import Image


def run(data, config=None):
    """将 image 切分为 patches (网格分块)"""
    file_content = data.get("file_content") if isinstance(data, dict) else data
    if not file_content:
        return data

    config = config or {}
    rows = config.get("rows", 2)
    cols = config.get("cols", 2)

    image = Image.open(io.BytesIO(file_content))
    w, h = image.size
    pw, ph = w // cols, h // rows

    patches = []
    positions = []
    for r in range(rows):
        for c in range(cols):
            left, top = c * pw, r * ph
            right = left + (pw if c < cols - 1 else w - left)
            bottom = top + (ph if r < rows - 1 else h - top)
            patch = image.crop((left, top, right, bottom))
            buf = io.BytesIO()
            patch.save(buf, format="PNG")
            patches.append(buf.getvalue())
            positions.append({"row": r, "col": c, "x": left, "y": top})

    return {
        "patches": patches,
        "positions": positions,
        "original_size": {"width": w, "height": h},
        "patches_shape": {"rows": rows, "cols": cols},
        "metadata": data.get("metadata", {}) if isinstance(data, dict) else {},
    }
