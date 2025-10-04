from dataclasses import dataclass

@dataclass(frozen=True)
class Coordinates:
    lat: float
    lon: float

    def validate(self) -> None:
        if not (-90.0 <= self.lat <= 90.0 and -180.0 <= self.lon <= 180.0):
            raise ValueError("Coordenadas inválidas (Fuera de rango).")
