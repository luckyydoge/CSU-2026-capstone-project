"""
负载测试：线性爬坡，monitor 包装
用法: uv run python test_load.py [initial_qps=0.5] [target_qps=10] [duration=120]
"""
import time
import sys
import os
import threading
from datetime import datetime, timezone
import ray
from monitor.proxy_class import monitor


def dummy_stage(data):
    import time
    tmp = 0
    t0 = time.time()
    while time.time() - t0 < 0.2:
         tmp += 1
    return {"status": "ok", "seq": data.get("seq")}


def listen_stop(stop_flag: threading.Event):
    input("按 Enter 停止提交...\n")
    stop_flag.set()


def main(initial_qps: float, target_qps: float, ramp_up_sec: int = 60, duration_sec: int = 1000, steady_sec: int = 40):
    ray.init(
        address="ray://127.0.0.1:10001",
        runtime_env={
            "py_modules": [os.path.join(os.path.dirname(__file__), "monitor")],
            "pip": ["psutil"],
        },
        ignore_reinit_error=True,
    )

    max_pending = 100

    refs = []
    completed = 0
    errors = 0
    seq = 0
    start = time.time()
    stop_flag = threading.Event()

    threading.Thread(target=listen_stop, args=(stop_flag,), daemon=True).start()

    print(f"负载测试：平稳 {steady_sec}s @ {initial_qps} QPS，然后爬坡 {ramp_up_sec}s -> {target_qps} QPS，总 {duration_sec}s")

    while time.time() - start < duration_sec and not stop_flag.is_set():
        elapsed = time.time() - start
        if elapsed < steady_sec:
            current_qps = initial_qps
            phase = "STEADY"
        else:
            ramp_elapsed = elapsed - steady_sec
            progress = min(ramp_elapsed / ramp_up_sec, 1.0)
            current_qps = initial_qps + (target_qps - initial_qps) * progress
            phase = "RAMP" if progress < 1.0 else "FULL"

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

        submit_time = datetime.now(timezone.utc)
        stage = "load_test"
        wrapped = monitor(stage_id=stage, submit_time=submit_time.isoformat(), num_cpus=1)(dummy_stage)
        remote_func = ray.remote(wrapped).options(num_cpus=0.2, label_selector={"test": "task"})
#        remote_func = ray.remote(wrapped).options(num_cpus=1, label_selector={"test": "task"})
        ref = remote_func.remote({"seq": seq})
        refs.append(ref)
        seq += 1

        interval = 1.0 / current_qps if current_qps > 0 else 1.0
        if stop_flag.is_set():
            break
        time.sleep(interval)

        print(f"\r[{elapsed:.0f}s][{phase}] QPS {current_qps:.1f}  提交 {seq}  完成 {completed}  错误 {errors}  排队 {len(refs)}", end="")
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
