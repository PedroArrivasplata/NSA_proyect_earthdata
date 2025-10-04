from dataclasses import dataclass

@dataclass
class AirQualityPrediction:
    dioxido_nitrogeno: float
    formaldehido: float
    indice_aerosol: float
    material_particulado: float
