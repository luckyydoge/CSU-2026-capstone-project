def run(input_data):
    import io
    
    file_content = input_data.get("file_content")
    metadata = input_data.get("metadata", {})
    
    if not file_content:
        return {"error": "No file content provided"}
    
    try:
        from PIL import Image
        import base64
        
        image = Image.open(io.BytesIO(file_content))
        original_size = image.size
        print(f"[resize_10] 原始图片大小: {original_size}")
        
        new_width = int(original_size[0] * 0.9)
        new_height = int(original_size[1] * 0.9)
        resized_image = image.resize((new_width, new_height), Image.LANCZOS)
        
        output = io.BytesIO()
        format_str = metadata.get("format", "PNG")
        resized_image.save(output, format=format_str)
        processed_content = output.getvalue()
        
        print(f"[resize_10] 调整后图片大小: {resized_image.size}")
        print(f"[resize_10] 处理后字节大小: {len(processed_content)} bytes")
        
        return {
            "file_content": processed_content,
            "metadata": {
                "original_size": original_size,
                "new_size": resized_image.size,
                "resize_ratio": 0.9,
                "format": format_str
            }
        }
    except Exception as e:
        print(f"[resize_10] 处理出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}
