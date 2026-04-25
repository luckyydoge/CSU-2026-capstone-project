# staged_code/cpuprocess.py

def run(input_data):
    """CPU密集处理阶段"""
    print(f"CPUProcess stage: processing data {input_data}")
    # 模拟CPU密集操作
    result = 0
    for i in range(10000000):
        result += i
    return {"processed_data": input_data, "stage": "cpuprocess", "cpu_result": result}
