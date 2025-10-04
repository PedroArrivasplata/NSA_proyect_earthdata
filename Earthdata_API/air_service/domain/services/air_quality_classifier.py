def status_no2(v):
    if v < 20: return "excellent"
    if v < 40: return "good"
    if v < 100: return "moderate"
    if v < 200: return "unhealthy"
    if v < 400: return "very_unhealthy"
    return "hazardous"

def status_hcho_ugm3(v):
    if v < 10: return "excellent"
    if v < 30: return "good"
    if v < 60: return "moderate"
    if v < 100: return "unhealthy"
    if v < 200: return "very_unhealthy"
    return "hazardous"

def status_pm25(v):
    if v < 5: return "excellent"
    if v < 12: return "good"
    if v < 35: return "moderate"
    if v < 55: return "unhealthy"
    if v < 150: return "very_unhealthy"
    return "hazardous"

def status_aerosol_index(v):
    if v < 0.1: return "excellent"
    if v < 0.3: return "good"
    if v < 0.7: return "moderate"
    if v < 1.0: return "unhealthy"
    if v < 1.5: return "very_unhealthy"
    return "hazardous"

def overall_from_worst(statuses):
    order = ["excellent","good","moderate","unhealthy","very_unhealthy","hazardous"]
    worst = max(statuses, key=lambda s: order.index(s))
    aqi_map = {"excellent":25,"good":50,"moderate":100,"unhealthy":150,"very_unhealthy":200,"hazardous":300}
    return worst, aqi_map[worst]
