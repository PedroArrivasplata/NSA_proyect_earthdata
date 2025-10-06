import os
import pandas as pd
import earthdataHCHO
from geopy.distance import distance

# -----funciones

def obtener_hcho_reciente_por_coordenada(df, latitud, longitud, tolerancia=0.5):
    """
    Devuelve el valor más reciente de formaldehído (HCHO) para una ubicación específica
    (latitud, longitud), excluyendo valores negativos y ordenando por tiempo descendente.
    
    Parámetros:
        df (pd.DataFrame): DataFrame con columnas ['latitud', 'longitud', 'tiempo', 'HCHO_molecules_per_cm2']
        latitud (float): latitud a consultar
        longitud (float): longitud a consultar
        tolerancia (float): margen de búsqueda en grados (default = 0.5)
    
    Retorna:
        pd.DataFrame con una sola fila (el valor más reciente) o None si no hay datos válidos.
    """

    # Filtrar por cercanía a la coordenada dada
    filtro = (
        (df["latitud"].between(latitud - tolerancia, latitud + tolerancia)) &
        (df["longitud"].between(longitud - tolerancia, longitud + tolerancia))
    )

    resultados = df[filtro].copy()

    # Eliminar valores negativos o no válidos
    resultados = resultados[resultados["HCHO_molecules_per_cm2"] > 0]

    if resultados.empty:
        print("⚠️ No se encontraron datos válidos (positivos) cercanos a las coordenadas dadas.")
        return None

    # Asegurar formato datetime
    resultados["tiempo"] = pd.to_datetime(resultados["tiempo"], errors="coerce")

    # Ordenar por tiempo descendente
    resultados = resultados.sort_values(by="tiempo", ascending=False)

    # Tomar el valor más reciente
    reciente = resultados.iloc[0]

    print(f"📍 Coordenadas consultadas: ({latitud}, {longitud}) ± {tolerancia}°")
    print(f"🕓 Fecha más reciente: {reciente['tiempo']}")
    print(f"🧪 HCHO más reciente: {reciente['HCHO_molecules_per_cm2']:.3e} molecules/cm²")

    return pd.DataFrame([reciente])[["tiempo", "latitud", "longitud", "HCHO_molecules_per_cm2"]]


# Parámetros
nombre_parquet = "hcho_combinado.parquet"
ruta_datos = "./hcho_data"

# Construir ruta completa del archivo
ruta_parquet = os.path.join(ruta_datos, nombre_parquet)

if not os.path.exists(ruta_parquet):
    print("⚠️ Archivo no encontrado. Ejecutando generación de datos desde earthdataHCHO...")
    earthdataHCHO.main()  # ✅ genera el archivo parquet

    # Verificar de nuevo después de ejecutar el generador
    if not os.path.exists(ruta_parquet):
        raise FileNotFoundError(f"❌ No se pudo generar el archivo: {ruta_parquet}")

# Leer el archivo .parquet
df_hcho = pd.read_parquet(ruta_parquet)

import os
import pandas as pd
import numpy as np

# Parámetros
nombre_parquet = "hcho_combinado.parquet"
ruta_datos = "./hcho_data"

ruta_parquet = os.path.join(ruta_datos, nombre_parquet)
if not os.path.exists(ruta_parquet):
    raise FileNotFoundError(f"⚠️ No se encontró el archivo: {ruta_parquet}")

# Leer el archivo
df_hcho = pd.read_parquet(ruta_parquet)

# Contar registros iniciales
inicial = len(df_hcho)

# Reemplazar infinitos por NaN
df_hcho.replace([np.inf, -np.inf], np.nan, inplace=True)

# Condiciones de limpieza
df_hcho = df_hcho[
    (df_hcho["latitud"].between(-90, 90)) &
    (df_hcho["longitud"].between(-180, 180)) &
    (df_hcho["HCHO_molecules_per_cm2"] > 0)
]

# Eliminar nulos
df_hcho.dropna(inplace=True)

# Contar registros finales
final = len(df_hcho)
print(f"🧹 Registros eliminados: {inicial - final}")
print(f"✅ Registros restantes: {final}")


# Ejemplo de uso:
resultado = obtener_hcho_reciente_por_coordenada(df_hcho, -5.18, -80.63)
if resultado is not None:
    hcho, lat_cercana, lon_cercana, fecha = resultado.iloc[0]
    print(f"HCHO: {hcho}, Coordenadas: ({lat_cercana}, {lon_cercana}), Fecha: {fecha}")
