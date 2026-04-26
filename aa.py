import time
import requests

url = "http://127.0.0.1:8428/api/v1/query"
params = {"query": 'quantile_over_time(0.99, proxy_queue_time_ms{stage_id="load_test"}[60s])'}
print(params["query"])

resp = requests.get(url, params=params)
data = resp.json()

print(time.time())

for item in data["data"]["result"]:
    print(item)
    metric = item["metric"]
    value = item["value"]
    stage_id = metric.get("stage_id", "unknown")
    p99_value = float(value[1])
    print(f"Stage: {stage_id}, P99 Queue Time: {p99_value} ms")
