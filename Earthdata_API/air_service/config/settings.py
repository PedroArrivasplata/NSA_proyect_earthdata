import os
from pathlib import Path

class Settings:
    BASE_DIR = Path(__file__).resolve().parents[2]
    MODEL_PATH: str = os.getenv("MODEL_PATH", str(BASE_DIR / "artifacts" / "air_model.joblib"))
