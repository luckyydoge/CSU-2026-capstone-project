# config.py
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

class Config:
    # Ray 集群地址（GCS 地址，格式为 IP:PORT，默认端口 10001）
    # 如果为 None 或空字符串，则启动本地 Ray 集群
    RAY_ADDRESS = os.getenv("RAY_ADDRESS", "ray://192.168.31.125:10001")

    # 是否使用本地模式（如果连接远程失败，是否回退到本地，建议 False 便于调试）
    FALLBACK_LOCAL = os.getenv("FALLBACK_LOCAL", "False").lower() == "true"

    # 完全本地模式：跳过 Ray，直接本地调用 stage 函数（用于无 Ray 环境的开发）
    LOCAL_MODE = os.getenv("LOCAL_MODE", "true").lower() == "true"

    # 是否启用资源预测器（覆盖部署配置中的 cpu/memory）
    USE_RESOURCE_PREDICTOR = os.getenv("USE_RESOURCE_PREDICTOR", "false").lower() == "true"

    # 代码目录配置
    STAGED_CODE_DIR = os.path.join(BASE_DIR, "staged_code")
    STRATEGY_CODE_DIR = os.path.join(BASE_DIR, "strategy_code")

CONFIG = Config()
