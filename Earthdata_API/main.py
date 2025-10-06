from fastapi import FastAPI
from air_service.app.container import Container
from air_service.adapters.web.api import get_router

container = Container()

app = FastAPI(title="Air Service", version="1.0")
app.include_router(get_router(container.predict_use_case), prefix="/api")

@app.get("/health")
def health():
    return {"status": "ok"}