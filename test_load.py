"""
负载测试：线性爬坡，monitor 包装
用法: uv run python test_load.py [initial_qps=0.5] [target_qps=10] [duration=120]
"""
import time
import sys
import threading
from datetime import datetime
import ray
from monitor.proxy_class import monitor


def dummy_stage(data):
    import time
    time.sleep(0.2)
    return {"status": "ok", "seq": data.get("seq")}


def listen_stop(stop_flag: threading.Event):
    input("按 Enter 停止提交...\n")
    stop_flag.set()


def main(initial_qps: float, target_qps: float, ramp_up_sec: int = 60, duration_sec: int = 300):
    ray.init(address="ray://127.0.0.1:10001", ignore_reinit_error=True)

    ramp_up_sec = 60
    max_pending = 100

    refs = []
    completed = 0
    errors = 0
    seq = 0
    start = time.time()
    stop_flag = threading.Event()

    threading.Thread(target=listen_stop, args=(stop_flag,), daemon=True).start()

    print(f"负载测试：{initial_qps} -> {target_qps} QPS 爬坡 {ramp_up_sec}s，总 {duration_sec}s")

    while time.time() - start < duration_sec and not stop_flag.is_set():
        elapsed = time.time() - start
        progress = min(elapsed / ramp_up_sec, 1.0)
        current_qps = initial_qps + (target_qps - initial_qps) * progress

        if refs:
            ready, _ = ray.wait(refs, num_returns=len(refs), timeout=0)
            for r in ready:
                try:
                    ray.get(r)
                    completed += 1
                except Exception:
                    errors += 1
                refs.remove(r)

        while len(refs) >= max_pending:
            ray.wait(refs, num_returns=1)

        submit_time = datetime.utcnow()
        stage = "load_test"
        wrapped = monitor(stage_id=stage, submit_time=submit_time.isoformat())(dummy_stage)
        remote_func = ray.remote(wrapped).options(num_cpus=1, label_selector={"label": "worker"})
        ref = remote_func.remote({"seq": seq})
        refs.append(ref)
        seq += 1

        interval = 1.0 / current_qps if current_qps > 0 else 1.0
        if stop_flag.is_set():
            break
        time.sleep(interval)

        print(f"\r[{elapsed:.0f}s] QPS {current_qps:.1f}  提交 {seq}  完成 {completed}  错误 {errors}  排队 {len(refs)}", end="")
        sys.stdout.flush()

    for r in refs:
        try:
            ray.get(r)
            completed += 1
        except Exception:
            errors += 1

    print(f"\n总计 提交 {seq}  完成 {completed}  错误 {errors}")


if __name__ == "__main__":
    args = [float(x) for x in sys.argv[1:3]] if len(sys.argv) > 2 else [0.5, 10.0]
    sec = int(sys.argv[3]) if len(sys.argv) > 3 else 120
    main(args[0], args[1], sec)
