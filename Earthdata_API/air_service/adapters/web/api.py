from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

class PredictRequest(BaseModel):
    latitude: float
    longitude: float

def get_router(predict_use_case):
    router = APIRouter(tags=["Predicción"])

    @router.post("/predict")
    def predict(req: PredictRequest):
        try:
            pred = predict_use_case.execute(req.latitude, req.longitude)
            return {
                "dioxido_nitrogeno": pred.dioxido_nitrogeno,
                "formaldehido": pred.formaldehido,
                "indice_aerosol": pred.indice_aerosol,
                "material_particulado": pred.material_particulado,
            }
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=str(ve))
        except Exception as e:
            raise HTTPException(status_code=500, detail="Error interno del servicio")

    return router
