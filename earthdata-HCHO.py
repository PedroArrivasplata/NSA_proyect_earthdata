from pathlib import Path
from datetime import datetime, timezone
from data_tempo_utils import (
    fetch_granule_data,
    setup_data_folder
)

class EarthDataHCHO:
    def __init__(self, root_dir: str = "./tempo_data", template_script: str = "./download_template.sh", concept_id="C3685912035-LARC_CLOUD"):
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

        self.folder = setup_data_folder(root_dir=self.root_dir)

        # Archivos necesarios para la descarga
        self.download_list = self.folder / "download_list.txt"
        self.download_script_template = Path(template_script)
        self.download_script = self.folder / "download_template.sh"
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

if __name__ == "__main__":
    # CONCEPTS_ID
    # C2930725014-LARC_CLOUD
    # C3685912035-LARC_CLOUD
    earthdata = EarthDataHCHO(concept_id="C3685912035-LARC_CLOUD")
    earthdata.download_data_today()


# ------------------------ Utilidades añadidas (no intrusivas) ------------------------
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

