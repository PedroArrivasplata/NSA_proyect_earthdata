
import os
import datetime as dt
import time
import logging
from typing import Optional

import numpy as np
import pandas as pd
import xarray as xr

# Dependencias opcionales
try:
    from harmony import BBox, Client, Collection, Request
    from harmony.config import Environment
    HARMONY_AVAILABLE = True
except Exception:
    HARMONY_AVAILABLE = False


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
    logging.info("Todos los archivos dentro de la carpeta han sido borrados.")


def descargar_harmony(start: dt.datetime, stop: dt.datetime, variables: list[str], carpeta: str, username: str, password: str) -> list[str]:
    """Usa Harmony para solicitar y descargar datos. Retorna la lista de rutas descargadas.

    Requiere la librería `harmony-py`. Si no está disponible, se lanzará RuntimeError.
    """
    if not HARMONY_AVAILABLE:
        raise RuntimeError("La librería 'harmony' no está disponible en el entorno.")

    client = Client(env=Environment.PROD, auth=(username, password))
    logging.info(f"Logging in as {username}")

    request = Request(
        collection=Collection(id="C3685912035-LARC_CLOUD"),
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


def leer_nc_a_dataframe(carpeta: str) -> pd.DataFrame:
    """Lee todos los archivos .nc en la carpeta y retorna un DataFrame consolidado.

    El DataFrame tiene columnas: ['latitud','longitud','tiempo','HCHO_molecules_per_cm2'].
    """
    if not os.path.exists(carpeta):
        raise FileNotFoundError(f"La carpeta '{carpeta}' no existe.")

    archivos_nc = sorted([os.path.join(carpeta, f) for f in os.listdir(carpeta) if f.endswith('.nc')])
    if not archivos_nc:
        raise FileNotFoundError("No se encontraron archivos .nc en la carpeta especificada.")

    df_total = pd.DataFrame()

    for archivo in archivos_nc:
        logging.info(f"Leyendo archivo: {archivo}")
        # Intentar abrir con xarray
        try:
            ds = xr.open_dataset(archivo)
            # Extraer variables si existen
            if 'geolocation/latitude' in ds:
                lat = ds['geolocation/latitude'].values
            else:
                lat = None
            if 'geolocation/longitude' in ds:
                lon = ds['geolocation/longitude'].values
            else:
                lon = None
            if 'product/vertical_column' in ds:
                hcho = ds['product/vertical_column'].values
            else:
                hcho = None
            # time puede estar en distintas ubicaciones
            if 'geolocation/time' in ds:
                time_var = ds['geolocation/time'].values
                time_attrs = ds['geolocation/time'].attrs
            elif 'time' in ds:
                time_var = ds['time'].values
                time_attrs = ds['time'].attrs if 'time' in ds else {}
            else:
                time_var = None
                time_attrs = {}
            ds.close()
        except Exception:
            # fallback a h5netcdf lectura directa si xr falla
            import h5netcdf
            with h5netcdf.File(archivo, 'r') as f:
                lat = f['geolocation/latitude'][:]
                lon = f['geolocation/longitude'][:]
                time_var = f['geolocation/time'][:]
                hcho = f['product/vertical_column'][:]
                time_attrs = getattr(f['geolocation/time'], 'attrs', {})

        # Asegurar que lat, lon, hcho no sean None
        if lat is None or lon is None or hcho is None:
            logging.warning(f"Archivo {archivo} no contiene las variables esperadas. Se omite.")
            continue

        # Alinear shapes
        if hcho.shape != lat.shape:
            min_shape = tuple(np.minimum(lat.shape, hcho.shape))
            lat = lat[tuple(slice(0, s) for s in min_shape)]
            lon = lon[tuple(slice(0, s) for s in min_shape)]
            hcho = hcho[tuple(slice(0, s) for s in min_shape)]

        # Expandir tiempo a la grilla si es necesario
        if time_var is None:
            time_expand = np.full_like(lat, np.nan, dtype=float)
        else:
            time_array = np.array(time_var)
            if time_array.ndim == 1:
                if time_array.size == lat.shape[0]:
                    time_expand = np.repeat(time_array[:, np.newaxis], lat.shape[1], axis=1)
                elif time_array.size == 1:
                    time_expand = np.full_like(lat, time_array[0], dtype=float)
                else:
                    # último recurso: usar la media
                    time_expand = np.full_like(lat, np.nan, dtype=float)
            else:
                time_expand = time_array

        # Convertir tiempo usando unidades si están presentes
        calendar = time_attrs.get('calendar', 'standard') if isinstance(time_attrs, dict) else 'standard'
        time_units = time_attrs.get('units', '') if isinstance(time_attrs, dict) else ''

        try:
            if 'since' in time_units:
                times_dt = xr.coding.times.decode_cf_datetime(time_expand, units=time_units, calendar=calendar)
            else:
                times_dt = pd.to_datetime(time_expand, unit='s', errors='coerce')
        except Exception:
            times_dt = pd.to_datetime(time_expand, unit='s', errors='coerce')

        df = pd.DataFrame({
            'latitud': lat.flatten(),
            'longitud': lon.flatten(),
            'tiempo': pd.Series(times_dt).values.flatten(),
            'HCHO_molecules_per_cm2': hcho.flatten()
        })

        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=['HCHO_molecules_per_cm2'])
        df_total = pd.concat([df_total, df], ignore_index=True)

    logging.info("Lectura completa de todos los archivos .nc")
    return df_total


def obtener_hcho_por_coordenada(df: pd.DataFrame, latitud: float, longitud: float, tolerancia: float = 0.5) -> Optional[pd.DataFrame]:
    """Devuelve los valores de HCHO para una coordenada dentro de una tolerancia.

    Retorna DataFrame ordenado por tiempo (más reciente primero) o None si no hay datos.
    """
    filtro = (
        (df['latitud'].between(latitud - tolerancia, latitud + tolerancia)) &
        (df['longitud'].between(longitud - tolerancia, longitud + tolerancia))
    )
    resultados = df[filtro].copy()
    if resultados.empty:
        logging.warning("No se encontraron datos cercanos a las coordenadas dadas.")
        return None

    # Asegurar formato datetime
    resultados['tiempo'] = pd.to_datetime(resultados['tiempo'], errors='coerce')
    resultados = resultados.sort_values(by='tiempo', ascending=False)

    logging.info(f"Coordenadas consultadas: ({latitud}, {longitud}) ± {tolerancia}° - Registros: {len(resultados)}")
    return resultados[['tiempo', 'latitud', 'longitud', 'HCHO_molecules_per_cm2']]


def obtener_hcho_reciente_por_coordenada(df: pd.DataFrame, latitud: float, longitud: float, tolerancia: float = 0.5) -> Optional[pd.DataFrame]:
    """Devuelve el registro más reciente de HCHO para una coordenada dada, excluyendo valores negativos."""
    filtro = (
        (df['latitud'].between(latitud - tolerancia, latitud + tolerancia)) &
        (df['longitud'].between(longitud - tolerancia, longitud + tolerancia))
    )
    resultados = df[filtro].copy()
    resultados = resultados[resultados['HCHO_molecules_per_cm2'] > 0]
    if resultados.empty:
        logging.warning("No se encontraron datos válidos (positivos) cercanos a las coordenadas dadas.")
        return None

    resultados['tiempo'] = pd.to_datetime(resultados['tiempo'], errors='coerce')
    resultados = resultados.sort_values(by='tiempo', ascending=False)
    reciente = resultados.iloc[0]

    logging.info(f"Fecha más reciente: {reciente['tiempo']} - HCHO: {reciente['HCHO_molecules_per_cm2']:.3e} molecules/cm²")
    return pd.DataFrame([reciente])[['tiempo', 'latitud', 'longitud', 'HCHO_molecules_per_cm2']]


def main():
    carpeta = '/datos_HCHO'

    # Ejemplo de uso: comentar o descomentar según necesidad
    # limpiar_carpeta(carpeta)

    # Si se desea descargar desde Harmony, descomente y configure credenciales
    # if HARMONY_AVAILABLE:
    #     ahora = dt.datetime.now()
    #     descargar_harmony(ahora - dt.timedelta(hours=1), ahora, ['geolocation/latitude','geolocation/longitude','product/vertical_column'], carpeta, username, password)

    df_total = leer_nc_a_dataframe(carpeta)

    # Mostrar resumen
    max_hcho = df_total['HCHO_molecules_per_cm2'].max()
    min_hcho = df_total['HCHO_molecules_per_cm2'].min()
    logging.info(f"Máximo HCHO: {max_hcho:.3e} molecules/cm²")
    logging.info(f"Mínimo HCHO: {min_hcho:.3e} molecules/cm²")

    # Ejemplo de consulta
    coord_lat, coord_lon = 22, -67
    reciente = obtener_hcho_reciente_por_coordenada(df_total, coord_lat, coord_lon)
    if reciente is not None:
        print(reciente)


if __name__ == '__main__':
    main()
