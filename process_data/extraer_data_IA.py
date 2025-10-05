import datetime as dt
import getpass
import logging
import os as os
import time
import numpy as np
import xarray as xr
import h5netcdf
import pandas as pd
import numpy as np
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
from xarray.plot.utils import label_from_attrs

from harmony import BBox, Client, Collection, Request
from harmony.config import Environment

from process_data.extraer_data_HCHO import HARMONY_AVAILABLE


def limpiar_carpeta(carpeta: str) -> None:
    """Elimina todos los archivos dentro de la carpeta dada.

    Levanta FileNotFoundError si la carpeta no existe.
    """
    if not os.path.exists(carpeta):
        raise FileNotFoundError(f"La carpeta '{carpeta}' no existe.")

    archivos = os.listdir(carpeta)
    for f in archivos:
        ruta_archivo = os.path.join(carpeta, f)
        if os.path.isfile(ruta_archivo):
            os.remove(ruta_archivo)
    print("✅ Todos los archivos dentro de la carpeta han sido borrados.")

def descargar_harmony(start: dt.datetime, stop: dt.datetime, variables: list[str], carpeta: str, username: str, password: str,idcollection:str) -> list[str]:
    """Usa Harmony para solicitar y descargar datos. Retorna la lista de rutas descargadas.

    Requiere la librería `harmony-py`. Si no está disponible, se lanzará RuntimeError.
    """
    if not HARMONY_AVAILABLE:
        raise RuntimeError("La librería 'harmony' no está disponible en el entorno.")

    client = Client(env=Environment.PROD, auth=(username, password))
    logging.info(f"Logging in as {username}")

    request = Request(
        collection=Collection(id=idcollection),
        temporal={"start": start, "stop": stop},
        variables=variables,
    )

    job_id = client.submit(request)
    logging.info(f"jobID = {job_id}")
    client.wait_for_processing(job_id, show_progress=True)

    results = client.download_all(job_id, directory=carpeta, overwrite=True)
    all_results_stored = [f.result() for f in results]
    logging.info(f"Number of result files: {len(all_results_stored)}")

    return all_results_stored

def extraer_datos_HCHO(carpeta: str, column_result: str, campo_resultante: str) -> pd.DataFrame:
    # Lista de archivos .nc
    archivos_nc = sorted([os.path.join(carpeta, f) for f in os.listdir(carpeta) if f.endswith(".nc")])
    # Crear un diccionario para almacenar los DataTree o Datasets
    datatree_dict = {}
    df_total=pd.DataFrame()
    for archivo in archivos_nc:
        print(f"\n📂 Leyendo archivo: {archivo}")

        # Intentar abrir como Dataset (más común)
        try:
            ds = xr.open_dataset(archivo)
        except Exception:
            # Si no funciona, abrir como DataTree
            ds = xr.open_datatree(archivo)

        datatree_dict[archivo] = ds

        # Mostrar rango de tiempo si existe la variable geolocation/time
        if 'geolocation/time' in ds.coords:
            print(f"🕓 Rango de tiempo: {ds['geolocation/time'].values.min()} - {ds['time'].values.max()}")

        with h5netcdf.File(archivo, 'r') as f:
            lat = f['geolocation/latitude'][:]
            lon = f['geolocation/longitude'][:]
            time = f['geolocation/time'][:]
            result = f[column_result][:]

            # Leer los atributos del tiempo
            time_attrs = f['geolocation/time'].attrs
            time_units = time_attrs.get('units', '')
            calendar = time_attrs.get('calendar', 'standard')

            print(f"🕓 Atributos de tiempo: units={time_units}, calendar={calendar}")

        # Asegurar que lat, lon, result tengan misma forma
        if result.shape != lat.shape:
            min_shape = tuple(np.minimum(lat.shape, result.shape))
            lat = lat[:min_shape[0], :min_shape[1]]
            lon = lon[:min_shape[0], :min_shape[1]]
            result = result[:min_shape[0], :min_shape[1]]

        # Expandir el tiempo a toda la grilla
        if len(time.shape) == 1:
            if len(time) == lat.shape[0]:
                time_expand = np.repeat(time[:, np.newaxis], lat.shape[1], axis=1)
            elif len(time) == 1:
                time_expand = np.full_like(lat, time[0], dtype=float)
            else:
                time_expand = np.full_like(lat, np.mean(time), dtype=float)
        else:
            time_expand = time

        # Convertir el tiempo correctamente usando las unidades del archivo
        try:
            if "since" in time_units:
                times_dt = xr.coding.times.decode_cf_datetime(time_expand, units=time_units, calendar=calendar)
            else:
                # Si no hay unidades, asumimos segundos desde 1970
                times_dt = pd.to_datetime(time_expand, unit='s', errors='coerce')
        except Exception as e:
            print(f"⚠️ No se pudo convertir el tiempo: {e}")
            times_dt = pd.to_datetime(time_expand, unit='s', errors='coerce')

        # Crear DataFrame
        df = pd.DataFrame({
            'latitud': lat.flatten(),
            'longitud': lon.flatten(),
            'tiempo': times_dt.flatten(),
            campo_resultante: result.flatten()
        })

        # Filtrar datos inválidos
        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=[campo_resultante])

        # Agregar al total
        df_total = pd.concat([df_total, df], ignore_index=True)

    print("\n✅ Lectura completa de todos los archivos .nc")

    print(df_total.head())
    return df_total

def obtener_hcho_por_coordenada(df, latitud, longitud, campo_resultante, tolerancia=0.5):
    """
    Devuelve los valores de formaldehído (HCHO) en todas las fechas disponibles
    para una ubicación específica (latitud, longitud), ordenados del más reciente al más antiguo.
    
    Parámetros:
        df (pd.DataFrame): DataFrame con columnas ['latitud', 'longitud', 'tiempo', 'HCHO_molecules_per_cm2']
        latitud (float): latitud a consultar
        longitud (float): longitud a consultar
        tolerancia (float): margen de búsqueda en grados (default = 0.05)
    
    Retorna:
        pd.DataFrame ordenado por tiempo (más reciente primero)
    """

    # Filtrar por cercanía a la coordenada dada
    filtro = (
        (df["latitud"].between(latitud - tolerancia, latitud + tolerancia)) &
        (df["longitud"].between(longitud - tolerancia, longitud + tolerancia))
    )
    
    resultados = df[filtro].copy()
    
    if resultados.empty:
        print("⚠️ No se encontraron datos cercanos a las coordenadas dadas.")
        return None

    # Convertir tiempo a formato datetime (en segundos desde época o ISO)
    resultados["tiempo"] = pd.to_datetime(resultados["tiempo"], errors="coerce", unit="s")

    # Ordenar por tiempo (más reciente primero)
    resultados = resultados.sort_values(by="tiempo", ascending=False)

    print(f"📍 Coordenadas consultadas: ({latitud}, {longitud}) ± {tolerancia}°")
    print(f"🕓 Rango temporal: {resultados['tiempo'].min()} → {resultados['tiempo'].max()}")
    print(f"🔢 Registros encontrados: {len(resultados)}")

    return resultados[["tiempo", "latitud", "longitud", campo_resultante]]

def obtener_hcho_reciente_por_coordenada(df, latitud, longitud, tolerancia=0.5,campo_resultante):
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
    resultados = resultados[resultados[campo_resultante] > 0]

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
    print(f"🧪 HCHO más reciente: {reciente[campo_resultante]:.3e} molecules/cm²")

    return pd.DataFrame([reciente])[["tiempo", "latitud", "longitud", campo_resultante]]
#-------------------------------------------------------------------------------
# Ruta de la carpeta
carpeta = "./process_data/datos_HCHO"

limpiar_carpeta(carpeta)

username = "pedro.arrivas"
password = "pEdrito02@123"

instante_actual = time.localtime()

annio = int(instante_actual.tm_year)
mes = int(instante_actual.tm_mon)
dia = int(instante_actual.tm_mday)
hora = int(instante_actual.tm_hour)
minuto = int(instante_actual.tm_min)

start = dt.datetime(annio, mes, dia, hora-1, minuto)
stop = dt.datetime(annio, mes, dia, hora, minuto)

campo_resultante = "HCHO_molecules_per_cm2"

idcollection="C3685912035-LARC_CLOUD"
variables=["geolocation/latitude","geolocation/longitude","product/vertical_column"]

rutas_descargadas = descargar_harmony(start, stop, variables, carpeta, username, password,idcollection)

df_final=extraer_datos_HCHO(carpeta, variables[-1],campo_resultante)

max_hcho = df_final[campo_resultante].max()
min_hcho = df_final[campo_resultante].min()
print(f"🔺 Máximo HCHO: {max_hcho:.3e} molecules/cm²")
print(f"🔻 Mínimo HCHO: {min_hcho:.3e} molecules/cm²")


resultados = obtener_hcho_por_coordenada(df_final, latitud=22, longitud=-67, campo_resultante=campo_resultante)
print(resultados.head())