# air_service/adapters/repositories/joblib_model_repository.py
import joblib
from typing import Any, Sequence

from air_service.domain.ports import AirQualityModelPort
from air_service.domain.value_objects import Coordinates
from air_service.domain.entities import AirQualityPrediction

class JoblibModelRepository(AirQualityModelPort):
    def __init__(self, model_path: str):
        self._model = joblib.load(model_path)

    def predict(self, coords: Coordinates) -> AirQualityPrediction:
        raw = self._model.predict(coords.lat, coords.lon)  # ← dict esperado

        if isinstance(raw, dict):
            return AirQualityPrediction(
                dioxido_nitrogeno=float(raw["Dioxido_de_nitrogeno"]),
                formaldehido=float(raw["Formaldehido"]),
                indice_aerosol=float(raw["Indice_de_aerosol"]),
                material_particulado=float(raw["Material_particulado"]),
            )

        raise ValueError("Formato de salida del modelo no reconocido (se esperaba dict).")
