from pathlib import Path
from datetime import datetime
from data_tempo_utils import (
    fetch_granule_data, 
    setup_data_folder
)
from datetime import datetime, timezone

def get_data_tempo_today(concept_id="C3685896708-LARC_CLOUD"):
    """
    Se filtra por concept id de la data de tempo
    """
    start_date = datetime.strptime("2025-10-04 00:00:00", "%Y-%m-%d %H:%M:%S")
    end_date   = datetime.strptime("2025-10-04 23:59:59", "%Y-%m-%d %H:%M:%S")

    root_dir = Path("./tempo_data").resolve()

    if not root_dir.exists():
        root_dir.mkdir(parents=True, exist_ok=True)
        print(f"📂 Carpeta creada: {root_dir}")
    else:
        print(f"📂 Carpeta ya existe: {root_dir}")

    folder = setup_data_folder(root_dir=root_dir)

    download_list = folder / "download_list.txt"
    download_script_template = Path("./download_template.sh") 
    download_script = folder / "download_template.sh"

    fetch_granule_data(
        concept_id=concept_id,
        start_date=start_date.replace(tzinfo=timezone.utc),
        end_date=end_date.replace(tzinfo=timezone.utc),
        folder=folder,
        download_list=download_list,
        download_script_template=download_script_template,
        download_script=download_script,
        skip_download=False,
        verbose=True,
        dry_run=False,
        only_one_file=False
    )

    print(f"✅ Data de TEMPO descargada en: {folder}")

def get_data_tempo_por_fechas(concept_id="C3685896708-LARC_CLOUD",fecha_inicio="", fecha_final=""):
    start_date = datetime.strptime(fecha_inicio+" 00:00:00","%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    end_date = datetime.strptime(fecha_final+" 23:59:59", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

    root_dir = Path("./tempo_data").resolve()

    if not root_dir.exists():
        root_dir.mkdir(parents=True, exist_ok=True)
        print(f"📂 Carpeta creada: {root_dir}")
    else:
        print(f"📂 Carpeta ya existe: {root_dir}")

    folder = setup_data_folder(root_dir=root_dir)

    download_list = folder / "download_list.txt"
    download_script_template = Path("./download_template.sh") 
    download_script = folder / "download_template.sh"

    print(f"📅 Descargando datos desde {start_date} hasta {end_date}")

    fetch_granule_data(
        concept_id=concept_id,
        start_date=start_date.replace(tzinfo=timezone.utc),
        end_date=end_date.replace(tzinfo=timezone.utc),
        folder=folder,
        download_list=download_list,
        download_script_template=download_script_template,
        download_script=download_script,
        skip_download=False,
        verbose=True,
        dry_run=False,
        only_one_file=False
    )

    print(f"✅ Data de TEMPO descargada en: {folder}")