import numpy as np
import random

class AirQualityModel:
    def predict(self, lat: float, lon: float) -> dict:
        base = np.sin(lat) + np.cos(lon)

        dn  = abs(base * 25 + random.uniform(10, 50))      # µg/m³
        hcho = abs(base * 0.005 + random.uniform(0.002, 0.02))  # mg/m³
        ai  = abs(base * 0.3  + random.uniform(0.5, 1.2))  # índice
        pm  = abs(base * 20   + random.uniform(5, 60))     # µg/m³

        return {
            "Dioxido_de_nitrogeno": round(dn, 2),
            "Formaldehido": round(hcho, 4),
            "Indice_de_aerosol": round(ai, 3),
            "Material_particulado": round(pm, 2),
        }
