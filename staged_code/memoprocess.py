# staged_code/memoprocess.py

def run(input_data):
    """内存密集处理阶段"""
    print(f"MemoProcess stage: processing data {input_data}")
    # 模拟内存密集操作
    large_list = [i for i in range(1000000)]  # 约4MB内存
    return {"processed_data": input_data, "stage": "memoprocess", "memo_size": len(large_list)}
