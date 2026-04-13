from prometheus_api_client import PrometheusConnect

# 1. 连接到 Prometheus (如果在 K8s 内部，使用 Service 地址)
prom = PrometheusConnect(url="http://localhost:9090", disable_ssl=True)


def get_submission_total_cpu_usage(submission_id: str):
    query = f'sum(ray_proxy_actor_cpu_usage{{submission_id="{submission_id}"}})'

    result = prom.custom_query(query)

    return result[0]["value"][1]

def get_submission_total_mem_usage(submission_id: str):
    query = f'sum(ray_proxy_actor_mem_usage{{submission_id="{submission_id}"}})'

    result = prom.custom_query(query)

    return result[0]["value"][1]

def get_proxy_actor_cpu_usage(proxy_actor_id: str):
    query = f'sum(ray_proxy_actor_mem_usage{{actor_id="{proxy_actor_id}"}})'

    result = prom.custom_query(query)

    return result[0]["value"][1]

def get_proxy_actor_mem_usage(proxy_actor_id: str):
    query = f'sum(ray_proxy_actor_mem_usage{{actor_id="{proxy_actor_id}"}})'

    result = prom.custom_query(query)

    return result[0]["value"][1]
