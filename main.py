import sys
import os

from config import CONFIG

os.makedirs(CONFIG.STAGED_CODE_DIR, exist_ok=True)
os.makedirs(CONFIG.STRATEGY_CODE_DIR, exist_ok=True)
if CONFIG.STAGED_CODE_DIR not in sys.path:
    sys.path.insert(0, CONFIG.STAGED_CODE_DIR)
if CONFIG.STRATEGY_CODE_DIR not in sys.path:
    sys.path.insert(0, CONFIG.STRATEGY_CODE_DIR)

from app.database import engine, Base
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

print("=" * 60)
print("初始化 Ray 连接...")
print(f"Ray 地址: {CONFIG.RAY_ADDRESS}")
print(f"回退本地模式: {CONFIG.FALLBACK_LOCAL}")

import ray

if CONFIG.RAY_ADDRESS:
    try:
        ray.init(address=CONFIG.RAY_ADDRESS, ignore_reinit_error=True)
        print(f"✅ 成功连接到 Ray 集群: {CONFIG.RAY_ADDRESS}")
    except Exception as e:
        print(f"❌ 连接 Ray 集群失败: {e}")
        if CONFIG.FALLBACK_LOCAL:
            print("回退到本地模式...")
            ray.init(ignore_reinit_error=True)
            print("✅ 已启动本地 Ray 集群")
        else:
            raise
else:
    ray.init(ignore_reinit_error=True)
    print("✅ 已启动本地 Ray 集群")

print("=" * 60)

from api.routes import router as api_router
from routers import monitor, db_api

app = FastAPI(title="协同推理平台 - 端边云协同系统")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

if not os.path.exists(STATIC_DIR):
    os.makedirs(STATIC_DIR)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/", response_class=HTMLResponse)
def read_index():
    index_path = os.path.join(STATIC_DIR, "index.html")
    if os.path.exists(index_path):
        with open(index_path, "r", encoding="utf-8") as f:
            return f.read()
    return "<html><body><h1>协同推理平台</h1><p>前端页面未找到</p></body></html>"

app.include_router(api_router)
app.include_router(monitor.router)
app.include_router(db_api.router)

@app.on_event("startup")
async def startup_event():
    Base.metadata.create_all(bind=engine)
    print("🚀 服务启动完成")
    print(f"📊 Ray 集群信息: {ray.cluster_resources()}")

@app.on_event("shutdown")
async def shutdown_event():
    print("🛑 关闭服务...")
    ray.shutdown()
    print("✅ Ray 已关闭")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
