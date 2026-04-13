from fastapi import FastAPI
from app.database import engine, Base
from routers.monitor import router as monitor_router

app = FastAPI()
app.include_router(monitor_router)

@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)

@app.get("/")
async def root():
    return {"message": "Hello World"}
