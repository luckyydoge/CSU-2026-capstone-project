"""
负载测试：每秒 2 个异步 Ray 任务，monitor 包装，每个 ~600ms
用法: uv run python test_load.py [duration_seconds=60]
"""
import time
import random
import sys
import threading
import ray
from monitor.proxy_class import monitor


def dummy_stage(data):
    import time
    import random
    start_time = time.time()
    tmp = 0
    while time.time() - start_time < 1:
        tmp += 1
        time.sleep(0.1)
    return {"status": "ok", "seq": data.get("seq"), "ans": tmp}


def listen_stop(stop_flag: threading.Event):
    input("按 Enter 停止提交...\n")
    stop_flag.set()


def main(duration_sec: int = 60):
    ray.init(address="ray://127.0.0.1:10001", ignore_reinit_error=True)

    refs = []
    completed = 0
    errors = 0
    seq = 0
    start = time.time()
    stop_flag = threading.Event()

    threading.Thread(target=listen_stop, args=(stop_flag,), daemon=True).start()

    print(f"负载测试：{duration_sec}s，异步提交 ~2 并发 (按 Enter 提前停止)...")

    while time.time() - start < duration_sec and not stop_flag.is_set():
        ready = []
        if refs:
            ready, _ = ray.wait(refs, num_returns=len(refs), timeout=0)
        for r in ready:
            try:
                ray.get(r)
                completed += 1
            except Exception:
                errors += 1
            refs.remove(r)


        stage = "load_test"
        wrapped = monitor(stage_id=stage)(dummy_stage)
        remote_func = ray.remote(wrapped).options(num_cpus=1, label_selector={"label": "worker"})
        ref = remote_func.remote({"seq": seq})
        refs.append(ref)
        seq += 1

        elapsed = time.time() - start
        print(f"\r[{elapsed:.0f}s] 提交 {seq}  完成 {completed}  错误 {errors}  进行中 {len(refs)}", end="")
        sys.stdout.flush()

        time.sleep(0.3)

    for r in refs:
        try:
            ray.get(r)
            completed += 1
        except Exception:
            errors += 1

    print(f"\n总计 提交 {seq}  完成 {completed}  错误 {errors}")


if __name__ == "__main__":
    sec = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    main(sec)
