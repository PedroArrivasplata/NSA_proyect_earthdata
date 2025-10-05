from air_service.domain.use_cases import PredictAirQualityUseCase
from air_service.adapters.repositories.joblib_model_repository import JoblibModelRepository
from air_service.config.settings import Settings

class Container:
    def __init__(self):
        self.settings = Settings()
        self.model_repo = JoblibModelRepository(self.settings.MODEL_PATH)
        self.predict_use_case = PredictAirQualityUseCase(self.model_repo)
