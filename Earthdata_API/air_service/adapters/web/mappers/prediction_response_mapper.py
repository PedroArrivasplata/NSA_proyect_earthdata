# air_service/adapters/web/mappers/prediction_response_mapper.py
from datetime import datetime, timezone
from air_service.domain.entities import AirQualityPrediction
from air_service.domain.services.air_quality_classifier import (
    status_no2, status_hcho_ugm3, status_pm25, status_aerosol_index, overall_from_worst
)
from air_service.domain.utils.units import mg_m3_to_ug_m3

def map_prediction_to_response(lat: float, lon: float, pred: AirQualityPrediction) -> dict:
    hcho_ug = mg_m3_to_ug_m3(pred.formaldehido)

    s_no2 = status_no2(pred.dioxido_nitrogeno)
    s_hcho = status_hcho_ugm3(hcho_ug)
    s_pm   = status_pm25(pred.material_particulado)      # tratado como PM2.5
    s_ai   = status_aerosol_index(pred.indice_aerosol)

    status_texts = {
        "excellent": ("Atmospheric aerosol measurement", "Excellent visibility and atmospheric quality", "Ideal conditions for outdoor activities"),
        "good":      ("Air quality generally acceptable", "Low risk", "Maintain adequate ventilation"),
        "moderate":  ("May affect sensitive groups", "Mild respiratory irritation", "Avoid intense outdoor exercise if sensitive"),
        "unhealthy": ("Unhealthy for sensitive groups", "Respiratory symptoms possible", "Limit outdoor activities"),
        "very_unhealthy": ("Very unhealthy", "More noticeable symptoms", "Avoid outdoor activities"),
        "hazardous": ("Hazardous", "Serious health risk", "Stay indoors with good filtration"),
    }
    def texts(s): return status_texts.get(s, status_texts["moderate"])

    _, hi_no2, rec_no2 = texts(s_no2)
    _, hi_hcho, rec_hcho = texts(s_hcho)
    _, hi_pm, rec_pm = texts(s_pm)
    _, hi_ai, rec_ai = texts(s_ai)

    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    indicators = [
        {"parameter":"NO2","value":round(pred.dioxido_nitrogeno,2),"unit":"µg/m³",
         "status":s_no2,"description":"Nitrogen Dioxide levels","health_impact":hi_no2,"recommendation":rec_no2},
        {"parameter":"Formaldehyde","value":round(hcho_ug,2),"unit":"µg/m³",
         "status":s_hcho,"description":"Formaldehyde concentration","health_impact":hi_hcho,"recommendation":rec_hcho},
        {"parameter":"PM2.5","value":round(pred.material_particulado,2),"unit":"µg/m³",
         "status":s_pm,"description":"Fine particulate matter","health_impact":hi_pm,"recommendation":rec_pm},
        {"parameter":"Aerosol_Index","value":round(pred.indice_aerosol,3),"unit":"AOD",
         "status":s_ai,"description":"Atmospheric aerosol measurement","health_impact":hi_ai,"recommendation":rec_ai},
    ]

    overall_status, aqi = overall_from_worst([s_no2, s_hcho, s_pm, s_ai])
    overall_desc = {
        "excellent":"Air quality is excellent for everyone",
        "good":"Air quality is satisfactory for most people",
        "moderate":"Air quality may be a concern for sensitive groups",
        "unhealthy":"Unhealthy for sensitive groups",
        "very_unhealthy":"Very unhealthy for everyone",
        "hazardous":"Hazardous conditions"
    }[overall_status]

    return {
        "success": True,
        "data": {
            "coordinates": {"latitude": lat, "longitude": lon},
            "timestamp": ts,
            "air_quality_indicators": indicators,
            "overall_assessment": {"status": overall_status, "aqi": aqi, "description": overall_desc}
        }
    }
