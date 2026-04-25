# staged_code/process.py

def run(input_data):
    """处理阶段"""
    print(f"Process stage: processing data {input_data}")
    return {"processed_data": input_data, "stage": "process"}
