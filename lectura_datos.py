import os
import pandas as pd

from geopy.distance import distance

# Parámetros
nombre_parquet = "hcho_combinado.parquet"
ruta_datos = "./hcho_data"

# Construir ruta completa del archivo
ruta_parquet = os.path.join(ruta_datos, nombre_parquet)

# Verificar que el archivo exista antes de leerlo
if not os.path.exists(ruta_parquet):
    raise FileNotFoundError(f"⚠️ No se encontró el archivo: {ruta_parquet}")

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

def obtener_hcho_mas_reciente(df, lat, lon):
    # Calcular distancia desde cada punto al solicitado
    df["distancia"] = df.apply(lambda row: distance((lat, lon), (row["latitud"], row["longitud"])).meters, axis=1)
    # Tomar el registro más cercano
    fila_cercana = df.loc[df["distancia"].idxmin()]
    # Filtrar por esa ubicación (en caso de que tenga varios tiempos) y obtener el más reciente
    df_filtrado = df[(df["latitud"] == fila_cercana["latitud"]) & (df["longitud"] == fila_cercana["longitud"])]
    fila_reciente = df_filtrado.sort_values("time", ascending=False).iloc[0]
    return fila_reciente["HCHO_molecules_per_cm2"], fila_reciente["latitud"], fila_reciente["longitud"], fila_reciente["time"]

# Ejemplo de uso:
hcho, lat_cercana, lon_cercana, fecha = obtener_hcho_mas_reciente(df_hcho, -5.18, -80.63)
print(f"HCHO: {hcho}, Coordenadas: ({lat_cercana}, {lon_cercana}), Fecha: {fecha}")
