def run(input_data):
    import io
    import os
    
    file_content = input_data.get("file_content")
    metadata = input_data.get("metadata", {})
    
    if not file_content:
        return {"error": "No file content provided"}
    
    print(f"[output] 收到文件内容，长度={len(file_content)} bytes")
    
    try:
        final_size = None
        try:
            from PIL import Image
            image = Image.open(io.BytesIO(file_content))
            final_size = image.size
            print(f"[output] 最终图片大小: {final_size}")
            print(f"[output] 图片格式: {image.format}")
        except ImportError:
            print(f"[output] PIL 不可用，不解析图片")
        
        print(f"[output] 元数据: {metadata}")
        
        return {
            "status": "success",
            "note": "File processed successfully (no save to disk due to permissions)",
            "output_size": len(file_content),
            "final_size": final_size,
            "metadata": metadata
        }
    except Exception as e:
        print(f"[output] 处理出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}
