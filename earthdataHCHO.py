from pathlib import Path
from datetime import datetime, timezone
from data_tempo_utils import (
    fetch_granule_data,
    setup_data_folder
)

# additional imports for merging
import xarray as xr
import pandas as pd
import numpy as np
import shutil
import glob
from pathlib import Path
import shutil

import numpy as np
import xarray as xr
import h5netcdf
import pyarrow

class EarthDataHCHO:
    def __init__(self, root_dir: str = "./tempo_data", data_dir: str | None = None, template_script: str = "./download_template.ps1", concept_id="C3685912035-LARC_CLOUD"):
        """
        Clase para descargar datos TEMPO HCHO de Earthdata (NASA).
        """
        self.start_date = datetime.strptime("2025-10-04 00:00:00", "%Y-%m-%d %H:%M:%S")
        self.end_date =  datetime.strptime("2025-10-04 23:59:59", "%Y-%m-%d %H:%M:%S")

        self.root_dir = Path(root_dir).resolve()

        if not self.root_dir.exists():
            self.root_dir.mkdir(parents=True, exist_ok=True)
            print(f"📂 Carpeta creada: {self.root_dir}")
        else:
            print(f"📂 Carpeta ya existe: {self.root_dir}")

        # Allow caller to pass a specific data directory name
        self.folder = setup_data_folder(data_dir=data_dir, root_dir=self.root_dir)

        # Archivos necesarios para la descarga
        self.download_list = self.folder / "download_list.txt"
        self.download_script_template = Path(template_script)
        self.download_script = self.folder / "download_template.ps1"
        self.concept_id = concept_id

    def download_data_today(self):
        """
        Descarga los datos de TEMPO HCHO para las fechas configuradas.
        """
        print(f"🚀 Descargando datos desde {self.start_date} hasta {self.end_date} ...")

        fetch_granule_data(
            concept_id=self.concept_id,
            start_date=self.start_date,
            end_date=self.end_date,
            folder=self.folder,
            download_list=self.download_list,
            download_script_template=self.download_script_template,
            download_script=self.download_script,
            skip_download=False,
            verbose=False,
            dry_run=False,
            only_one_file=False
        )

        print(f"✅ Data de TEMPO (HCHO) descargada en: {self.folder}")

        # consolidation removed from class; use module function merge_nc_to_parquet(folder, out_path, variables)

    def download_data_by_date(self, start: str, end: str,
                            skip_download=False, verbose=True,
                            dry_run=False, only_one_file=False):
        """
        Descarga la data de acuerdo a la fecha ingresada.
        
        Parámetros:
        ----------
        start : str
            Fecha de inicio en formato YYYY-MM-DD
        end : str
            Fecha de fin en formato YYYY-MM-DD
        """
        try:
            date_start = datetime.strptime(start + " 00:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            date_end = datetime.strptime(end + " 23:59:59", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            raise ValueError("❌ Formato de fecha inválido. Usa YYYY-MM-DD")
        fetch_granule_data(
            concept_id=self.concept_id,
            start_date=date_start,
            end_date=date_end,
            folder=self.folder,
            download_list=self.download_list,
            download_script_template=self.download_script_template,
            download_script=self.download_script,
            skip_download=skip_download,
            verbose=verbose,
            dry_run=dry_run,
            only_one_file=only_one_file
        )

        print(f"📅 Descargando datos desde {date_start} hasta {date_end}")
        print(f"✅ Data de TEMPO (HCHO) descargada en: {self.folder}")

        # consolidation removed from class; use module function merge_nc_to_parquet(folder, out_path, variables)

    # Wrappers que crean un .netrc temporal si se encuentran variables de entorno
    def download_data_today_with_netrc(self):
        """Wrapper que crea .netrc desde EARTHDATA_USER/EARTHDATA_PASS (si existen) y llama a download_data_today()."""
        created = create_netrc_from_env()
        try:
            return self.download_data_today()
        finally:
            if created:
                remove_netrc_if_created()

    def download_data_by_date_with_netrc(self, start: str, end: str, **kwargs):
        """Wrapper que crea .netrc desde EARTHDATA_USER/EARTHDATA_PASS (si existen) y llama a download_data_by_date()."""
        created = create_netrc_from_env()
        try:
            return self.download_data_by_date(start, end, **kwargs)
        finally:
            if created:
                remove_netrc_if_created()

    # class no longer performs cleaning/merging; use module functions clean_folder(folder) and
    # merge_nc_to_parquet(folder, out_path, variables)

# main block moved to end of file to ensure helper functions are defined before use


# ------------------------ Utilidades añadidas (no intrusivas) ------------------------
import os
from pathlib import Path

_NETRC_CREATED_BY_SCRIPT = None

def create_netrc_from_env() -> bool:
    """Crea ~/.netrc (en Windows $USERPROFILE) usando EARTHDATA_USER/EARTHDATA_PASS si están definidas.

    Retorna True si creó el archivo (y lo marcó para borrado posterior), False si no hizo nada.
    """
    global _NETRC_CREATED_BY_SCRIPT
    user = os.environ.get('EARTHDATA_USER')
    pwd = os.environ.get('EARTHDATA_PASS')
    if not user or not pwd:
        return False

    netrc_path = Path(os.path.expanduser('~')) / '.netrc'
    if netrc_path.exists():
        # No sobreescribir si ya existe
        return False

    content = f"machine urs.earthdata.nasa.gov login {user} password {pwd}\n"
    netrc_path.write_text(content, encoding='ascii')

    # Ajustar permisos en Windows: quitar herencia y dejar sólo lectura al usuario
    try:
        import subprocess
        subprocess.run(['icacls', str(netrc_path), '/inheritance:r'], check=False)
        subprocess.run(['icacls', str(netrc_path), '/grant:r', f"{os.environ.get('USERNAME')}:R"], check=False)
    except Exception:
        # Si falla, no es crítico; devolveremos True para intentar borrar luego
        pass

    _NETRC_CREATED_BY_SCRIPT = str(netrc_path)
    return True


def remove_netrc_if_created() -> None:
    """Borra el .netrc creado por create_netrc_from_env() si existe.
    No borra .netrcs previos a la ejecución.
    """
    global _NETRC_CREATED_BY_SCRIPT
    if not _NETRC_CREATED_BY_SCRIPT:
        return
    try:
        p = Path(_NETRC_CREATED_BY_SCRIPT)
        if p.exists():
            p.unlink()
    finally:
        _NETRC_CREATED_BY_SCRIPT = None

def cmr_get_latest_granule_for_bbox(collection_concept_id: str, bbox: tuple, cmr_base: str = "https://cmr.earthdata.nasa.gov/search/granules.json") -> dict | None:
    """Consulta CMR y devuelve el metadato del granule más reciente que intersecta el bbox.

    bbox = (min_lon, min_lat, max_lon, max_lat)
    Retorna el diccionario del 'entry' de CMR o None si no hay resultados.
    """
    import requests

    bbox_str = ",".join(map(str, bbox))
    params = {
        "collection_concept_id": collection_concept_id,
        "bounding_box": bbox_str,
        "page_size": 1,
        "sort_key": "-start_date",
    }

    r = requests.get(cmr_base, params=params, timeout=30)
    r.raise_for_status()
    results = r.json()
    items = results.get("feed", {}).get("entry", [])
    if not items:
        return None
    return items[0]


def harmony_download_subset_for_granule(granule_entry: dict, collection_id: str, variables: list[str], bbox: tuple, out_dir: str, username: str | None = None, password: str | None = None) -> list[str] | None:
    """Usa harmony-py para solicitar un subset del granule dado (si harmony está disponible).

    - granule_entry: diccionario devuelto por CMR (entry)
    - collection_id: concept id de la colección (ej. C3685912035-LARC_CLOUD)
    - variables: lista de variables a incluir en el subset
    - bbox: tupla (min_lon, min_lat, max_lon, max_lat)
    - out_dir: carpeta donde guardar
    Si harmony no está instalado, retorna None.
    """
    # import locales para no tocar cabeceras
    try:
        from harmony import Client, Collection, Request
        from harmony.config import Environment
    except Exception:
        return None

    # Extraer temporal del granule
    time_start = granule_entry.get("time_start") or granule_entry.get("start_time")
    time_end = granule_entry.get("time_end") or granule_entry.get("end_time")

    from datetime import datetime

    def _parse_iso(s):
        if s is None:
            return None
        try:
            # datetime.fromisoformat acepta offsets en Python 3.11+
            return datetime.fromisoformat(s)
        except Exception:
            # fallback: try common formats
            try:
                return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
            except Exception:
                return None

    start_dt = _parse_iso(time_start)
    end_dt = _parse_iso(time_end)

    # Preparar cliente
    auth = (username, password) if username and password else None
    client = Client(env=Environment.PROD, auth=auth)

    req_kwargs = {
        "collection": Collection(id=collection_id),
        "variables": variables,
    }
    if start_dt and end_dt:
        req_kwargs["temporal"] = {"start": start_dt, "stop": end_dt}
    # spatial/bbox puede aceptarse por algunas versiones de harmony-py
    try:
        req_kwargs["bbox"] = bbox
    except Exception:
        # no hacer nada si la versión no lo acepta
        pass

    request = Request(**req_kwargs)
    job_id = client.submit(request)
    client.wait_for_processing(job_id, show_progress=True)
    results = client.download_all(job_id, directory=out_dir, overwrite=True)
    all_results = [f.result() for f in results]
    return all_results


def process_zones_latest_granule(zones: list[tuple], collection_id: str, variables: list[str], out_dir: str, username: str | None = None, password: str | None = None) -> dict:
    """Para cada bbox en zones obtiene el último granule (CMR) y (si puede) descarga el subset via Harmony.

    zones: lista de bbox tuples (min_lon, min_lat, max_lon, max_lat)
    Retorna un dict mapping zona -> {"granule": entry or None, "files": list[str] or None}
    """
    results = {}
    for bbox in zones:
        granule = cmr_get_latest_granule_for_bbox(collection_id, bbox)
        files = None
        if granule is not None:
            files = harmony_download_subset_for_granule(granule, collection_id, variables, bbox, out_dir, username, password)
        results[bbox] = {"granule": granule, "files": files}
    return results


# -------------- Funciones de limpieza y consolidacion --------------
def _safe_open_dataset(path):
    try:
        return xr.open_dataset(path)
    except Exception:
        try:
            import h5netcdf
            return xr.open_dataset(path, engine='h5netcdf')
        except Exception:
            raise


def _extract_vars_from_ds(ds, path):
    # Try common variable names
    var_map = {
        'lat': None,
        'lon': None,
        'time': None,
        'hcho': None
    }
    for name in ds.variables:
        lname = name.lower()
        if 'geolocation/latitude' in name or 'latitude' == lname or 'lat' == lname:
            var_map['lat'] = ds[name].values
        if 'geolocation/longitude' in name or 'longitude' == lname or 'lon' == lname:
            var_map['lon'] = ds[name].values
        if 'geolocation/time' in name or 'time' == lname:
            var_map['time'] = ds[name].values
        if 'product/vertical_column' in name or 'vertical_column' in lname or 'hcho' in lname:
            var_map['hcho'] = ds[name].values

    # If any variable is missing, try coords
    if var_map['lat'] is None and 'geolocation/latitude' in ds.coords:
        var_map['lat'] = ds.coords['geolocation/latitude'].values
    if var_map['lon'] is None and 'geolocation/longitude' in ds.coords:
        var_map['lon'] = ds.coords['geolocation/longitude'].values

    if var_map['hcho'] is None:
        raise ValueError(f"Archivo {path} no contiene variable HCHO esperada")

    return var_map


def clean_folder(folder):
    folder = Path(folder)  # ✅ Convierte a Path por si viene como string

    if not folder.exists():
        print(f"📂 La carpeta no existe: {folder}")
        return

    for item in folder.iterdir():
        try:
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
        except Exception as e:
            print(f"⚠️ No se pudo eliminar {item}: {e}")

    print(f"🧹 Carpeta limpiada: {folder}")


def procesar_nc_a_parquet(
    root_dir, 
    data_dir, 
    variables, 
    nombre_resultado="resultado", 
    unidades_resultado="", 
    output_name="datos_resultado.parquet"
):
    """
    Lee archivos .nc dentro de una carpeta, extrae variables específicas y guarda los datos combinados en un .parquet.

    Parámetros:
    ------------
    root_dir : str
        Directorio raíz donde se encuentra la carpeta de datos.
    data_dir : str
        Nombre de la carpeta que contiene los archivos .nc.
    variables : list
        Lista con los nombres completos de las variables a extraer. 
        Ejemplo: ["geolocation/latitude", "geolocation/longitude", "geolocation/time", "product/vertical_column"]
        La última variable será tomada como el valor del resultado.
    nombre_resultado : str
        Nombre del campo resultado en el DataFrame (por defecto "resultado").
    unidades_resultado : str
        Unidades del resultado, se agregan como atributo en el archivo parquet.
    output_name : str
        Nombre del archivo parquet de salida.

    Retorna:
    ---------
    str
        Ruta completa del archivo .parquet generado.
    """

    carpeta = os.path.join(root_dir, data_dir)

    if not os.path.exists(carpeta):
        raise FileNotFoundError(f"La carpeta '{carpeta}' no existe.")

    archivos_nc = sorted([os.path.join(carpeta, f) for f in os.listdir(carpeta) if f.endswith(".nc")])
    if not archivos_nc:
        raise FileNotFoundError(f"No se encontraron archivos .nc en '{carpeta}'.")

    df_total = pd.DataFrame()

    # Separar variables base y variable de resultado
    *vars_base, var_resultado = variables

    for archivo in archivos_nc:
        print(f"📂 Leyendo archivo: {archivo}")

        with h5netcdf.File(archivo, 'r') as f:
            datos = {}
            for var in vars_base + [var_resultado]:
                try:
                    datos[var] = f[var][:]
                except KeyError:
                    raise KeyError(f"La variable '{var}' no se encontró en el archivo {archivo}.")

            # Atributos del tiempo (si existe)
            time_attrs = f[vars_base[-1]].attrs if "time" in vars_base[-1] else {}
            time_units = time_attrs.get('units', '')
            calendar = time_attrs.get('calendar', 'standard')

        # Renombrar variables base
        lat = datos[vars_base[0]]
        lon = datos[vars_base[1]]
        time = datos[vars_base[2]] if len(vars_base) > 2 else None
        valor = datos[var_resultado]

        # Ajustar formas si difieren
        if valor.shape != lat.shape:
            min_shape = tuple(np.minimum(lat.shape, valor.shape))
            lat = lat[:min_shape[0], :min_shape[1]]
            lon = lon[:min_shape[0], :min_shape[1]]
            valor = valor[:min_shape[0], :min_shape[1]]

        # Expandir el tiempo
        if time is not None:
            if len(time.shape) == 1:
                if len(time) == lat.shape[0]:
                    time_expand = np.repeat(time[:, np.newaxis], lat.shape[1], axis=1)
                elif len(time) == 1:
                    time_expand = np.full_like(lat, time[0], dtype=float)
                else:
                    time_expand = np.full_like(lat, np.mean(time), dtype=float)
            else:
                time_expand = time

            # Convertir tiempo
            try:
                if "since" in time_units:
                    times_dt = xr.coding.times.decode_cf_datetime(time_expand, units=time_units, calendar=calendar)
                else:
                    times_dt = pd.to_datetime(time_expand, unit='s', errors='coerce')
            except Exception as e:
                print(f"⚠️ Error al convertir tiempo: {e}")
                times_dt = pd.to_datetime(time_expand, unit='s', errors='coerce')
        else:
            times_dt = np.full_like(lat, np.nan)

        # Crear DataFrame
        df = pd.DataFrame({
            'latitud': lat.flatten(),
            'longitud': lon.flatten(),
            'tiempo': times_dt.flatten(),
            nombre_resultado: valor.flatten()
        })

        # Limpiar valores no válidos
        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=[nombre_resultado])
        df_total = pd.concat([df_total, df], ignore_index=True)

    # Guardar en formato parquet con metadatos
    output_path = os.path.join(root_dir, output_name)
    df_total.to_parquet(output_path, index=False)

    print(f"\n✅ Archivo Parquet generado: {output_path}")
    print(f"📊 Total de registros: {len(df_total)}")
    print(f"⚙️ Campo resultado: {nombre_resultado} ({unidades_resultado})")

    return output_path

def main():
    # CONCEPTS_ID
    # C2930725014-LARC_CLOUD
    # C3685912035-LARC_CLOUD
    root_dir="./hcho_data"
    data_dir="data_today"
    clean_folder(root_dir)
    # Definición de rutas
    root_dir = Path("./hcho_data").resolve()
    data_dir = root_dir / "data_today"

    # Crear root_dir si no existe
    if not root_dir.exists():
        root_dir.mkdir(parents=True, exist_ok=True)
        print(f"📁 Carpeta creada: {root_dir}")
    else:
        print(f"📁 Carpeta ya existe: {root_dir}")

    # Crear data_dir dentro de root_dir
    if not data_dir.exists():
        data_dir.mkdir(parents=True, exist_ok=True)
        print(f"📂 Subcarpeta creada: {data_dir}")
    else:
        print(f"📂 Subcarpeta ya existe: {data_dir}")


    earthdata = EarthDataHCHO(concept_id="C3685912035-LARC_CLOUD",root_dir="./hcho_data",data_dir="data_today")

    # Download into the configured data folder
    earthdata.download_data_today()

    # Merge downloaded .nc files into a single parquet file using the
    # module-level function. The class intentionally does not perform
    # cleaning/merging — call these utilities explicitly.
    out_parquet = earthdata.root_dir / "hcho_merged.parquet"

    root = str(root_dir)
    data_folder = str(data_dir)

    variables = [
        "geolocation/latitude",
        "geolocation/longitude",
        "geolocation/time",
        "product/vertical_column"
    ]

    procesar_nc_a_parquet(
        root_dir=root,
        data_dir=data_folder,
        variables=variables,
        nombre_resultado="HCHO_molecules_per_cm2",
        unidades_resultado="molec/cm²",
        output_name="hcho_combinado.parquet"
    )


if __name__ == "__main__":
    main()




