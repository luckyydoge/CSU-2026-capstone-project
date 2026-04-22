# config.py
import os

class Config:
    # Ray 集群地址（GCS 地址，格式为 IP:PORT，默认端口 10001）
    # 如果为 None 或空字符串，则启动本地 Ray 集群
    RAY_ADDRESS = os.getenv("RAY_ADDRESS", "ray://192.168.31.125:10001")
    
    # 是否使用本地模式（如果连接远程失败，是否回退到本地，建议 False 便于调试）
    FALLBACK_LOCAL = os.getenv("FALLBACK_LOCAL", "False").lower() == "true"

CONFIG = Config()