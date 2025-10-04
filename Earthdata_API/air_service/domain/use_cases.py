from .value_objects import Coordinates
from .entities import AirQualityPrediction
from .ports import AirQualityModelPort

class PredictAirQualityUseCase:
    def __init__(self, model_port: AirQualityModelPort):
        self._model = model_port

    def execute(self, lat: float, lon: float) -> AirQualityPrediction:
        coords = Coordinates(lat, lon)
        coords.validate()
        return self._model.predict(coords)
