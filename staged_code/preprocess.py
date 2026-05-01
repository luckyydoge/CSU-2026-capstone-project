def run(input_data):
    data = input_data if isinstance(input_data, dict) else {"value": input_data}
    image_data = data.get("file_content", b"")
    return {**data, "preprocessed": True, "size": len(image_data) if image_data else 0}
