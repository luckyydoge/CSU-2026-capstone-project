from fastapi import FastAPI
from app.database import engine, Base
from routers import monitor

app = FastAPI()


@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)


app.include_router(monitor.router)


@app.get("/")
async def root():
    return {"message": "Hello World"}
