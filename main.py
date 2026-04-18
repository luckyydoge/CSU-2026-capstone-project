import time
import os
import ray
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from offload.factory import HandlerFactory
from offload.scheduler import EdgeOrchestrator

app = FastAPI()
orchestrator = EdgeOrchestrator()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
if os.path.exists(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/", response_class=HTMLResponse)
async def read_index():
    with open(os.path.join(STATIC_DIR, "index.html"), "r", encoding="utf-8") as f:
        return f.read()

@app.post("/execute-task")
async def handle_universal_task(
    task_type: str = Form(...),
    file: UploadFile = File(...)
):
    try:
        handler = HandlerFactory.create_handler(task_type)
        raw_bytes = await file.read()
        processed_data = handler.preprocess(raw_bytes)

        start_time = time.perf_counter()
        final_result, tags = await orchestrator.execute(handler, processed_data)
        duration = time.perf_counter() - start_time

        try:
            current_node = f"Ray-Node-{ray.get_runtime_context().get_node_id()[:6]}"
        except:
            current_node = "Edge-Cluster-Alpha"

        return {
            "status": "success",
            "task_type": task_type,
            "tags": tags,
            "duration": f"{duration:.3f}",
            "node_info": current_node,
            "result": final_result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
