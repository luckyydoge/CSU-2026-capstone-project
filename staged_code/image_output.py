def run(input_data):
    import io
    import os

    file_content = input_data.get("file_content")
    metadata = input_data.get("metadata", {})

    if not file_content:
        return {"error": "No file content provided"}

    try:
        from PIL import Image

        image = Image.open(io.BytesIO(file_content))
        print(f"[output] 最终图片大小: {image.size}")
        print(f"[output] 图片格式: {image.format}")
        print(f"[output] 元数据: {metadata}")

        output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "outputs")
        os.makedirs(output_dir, exist_ok=True)

        output_filename = f"resized_image_{metadata.get('resize_ratio', 'final')}.png"
        output_path = os.path.join(output_dir, output_filename)

        image.save(output_path)
        print(f"[output] 图片已保存到: {output_path}")

        file_size = os.path.getsize(output_path)
        print(f"[output] 输出文件大小: {file_size} bytes")

        return {
            "status": "success",
            "output_path": output_path,
            "output_size": file_size,
            "final_size": image.size,
            "metadata": metadata
        }
    except Exception as e:
        print(f"[output] 处理出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}
