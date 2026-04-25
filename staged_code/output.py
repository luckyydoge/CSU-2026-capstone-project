# staged_code/output.py

def run(input_data):
    """输出阶段"""
    print(f"Output stage: processing data {input_data}")
    return {"final_result": input_data, "stage": "output"}
