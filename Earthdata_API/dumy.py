# train_fake_model.py
import joblib
import numpy as np
import random

class AirQualityModel:
    """
    Modelo ficticio para simular valores de contaminación ambiental
    basados en latitud y longitud.
    """

    def predict(self, X):
        """
        Recibe una lista de pares [lat, lon] y devuelve valores simulados.
        """
        results = []

        for lat, lon in X:
            # Simulamos variaciones usando funciones trigonométricas y ruido aleatorio
            base = np.sin(lat) + np.cos(lon)

            dioxido_nitrogeno = abs(base * 25 + random.uniform(10, 50))  # µg/m³
            formaldehido = abs(base * 0.005 + random.uniform(0.002, 0.02))  # mg/m³
            indice_aerosol = abs(base * 0.3 + random.uniform(0.5, 1.2))  # índice adimensional
            material_particulado = abs(base * 20 + random.uniform(5, 60))  # µg/m³

            results.append({
                "Dioxido_de_nitrogeno": round(dioxido_nitrogeno, 2),
                "Formaldehido": round(formaldehido, 4),
                "Indice_de_aerosol": round(indice_aerosol, 3),
                "Material_particulado": round(material_particulado, 2)
            })

        return results


if __name__ == "__main__":
    # Crear el modelo ficticio
    model = AirQualityModel()

    # Guardar el modelo en un archivo .joblib
    joblib.dump(model, "dumy.joblib")

    print("✅ Modelo ficticio guardado como 'dumy.joblib'")
