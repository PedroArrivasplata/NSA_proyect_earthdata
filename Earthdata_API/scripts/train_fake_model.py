from joblib import dump
from air_service.ml.fake_air_quality_model import AirQualityModel

if __name__ == "__main__":
    dump(AirQualityModel(), "artifacts/air_model.joblib")
    print("✅ Modelo guardado en artifacts/air_model.joblib")