from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from air_service.adapters.web.mappers.prediction_response_mapper import map_prediction_to_response

class PredictRequest(BaseModel):
    latitude: float = Field(..., description="Latitud en grados decimales (-90 a 90)")
    longitude: float = Field(..., description="Longitud en grados decimales (-180 a 180)")

def get_router(predict_use_case):
    router = APIRouter(tags=["Predicción"])

    @router.post("/predict")
    def predict(req: PredictRequest):
        try:
            pred = predict_use_case.execute(req.latitude, req.longitude)
            return map_prediction_to_response(req.latitude, req.longitude, pred)
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=str(ve))
        except Exception:
            raise HTTPException(status_code=500, detail="Error interno del servidor")

    return router
