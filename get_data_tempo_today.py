from pathlib import Path
from datetime import datetime
from data_tempo_utils import (
    fetch_granule_data, 
    setup_data_folder
)
from datetime import datetime, timedelta, timezone

today = datetime.utcnow()

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
    concept_id="C3685896708-LARC_CLOUD",
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