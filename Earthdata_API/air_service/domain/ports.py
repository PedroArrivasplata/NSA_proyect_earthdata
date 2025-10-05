from abc import ABC, abstractmethod
from .value_objects import Coordinates
from .entities import AirQualityPrediction

class AirQualityModelPort(ABC):
    @abstractmethod
    def predict(self, coords: Coordinates) -> AirQualityPrediction:
        """Devuelve una predicción para las coordenadas."""
