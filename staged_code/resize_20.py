def run(input_data):
    import io

    import time
    
    start = time.time()
    tmp = 0

    while time.time() - start < 2:
        tmp += 1  # 空循环，疯狂占 CPU
    print(tmp)
    
    file_content = input_data.get("file_content")
    metadata = input_data.get("metadata", {})
    
    if not file_content:
        return {"error": "No file content provided"}
    
    print(f"[resize_20] 收到文件内容，长度={len(file_content)} bytes")
    
    try:
        from PIL import Image
        import base64
        
        image = Image.open(io.BytesIO(file_content))
        original_size = image.size
        print(f"[resize_20] 原始图片大小: {original_size}")
        
        new_width = int(original_size[0] * 0.8)
        new_height = int(original_size[1] * 0.8)
        resized_image = image.resize((new_width, new_height), Image.LANCZOS)
        
        output = io.BytesIO()
        format_str = metadata.get("format", "PNG")
        resized_image.save(output, format=format_str)
        processed_content = output.getvalue()
        
        print(f"[resize_20] 调整后图片大小: {resized_image.size}")
        print(f"[resize_20] 处理后字节大小: {len(processed_content)} bytes")
        
        return {
            "file_content": processed_content,
            "metadata": {
                "original_size": metadata.get("original_size", original_size),
                "new_size": resized_image.size,
                "resize_ratio": 0.8,
                "format": format_str
            }
        }
    except ImportError as e:
        print(f"[resize_20] PIL 不可用，直接返回原始文件内容: {e}")
        return {
            "file_content": file_content,
            "metadata": {
                "note": "PIL not available, returned original content",
                "original_size": len(file_content)
            }
        }
    except Exception as e:
        print(f"[resize_20] 处理出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}
