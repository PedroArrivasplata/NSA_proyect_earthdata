from pathlib import Path
from datetime import datetime
from data_tempo_utils import (
    fetch_granule_data,
    setup_data_folder,
    get_date_limits
)

class EarthDataNO2:
    def __init__(self, root_dir: str = "./tempo_data", template_script: str = "./download_template.sh"):
        """
        Clase para descargar datos TEMPO NO2 de Earthdata (NASA).
        """
        self.today = datetime.utcnow()
        self.start_date, self.end_date, self.last_downloaded_time = get_date_limits()

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

    def download_data(self, skip_download=False, verbose=True, dry_run=False, only_one_file=False):
        """
        Descarga los datos de TEMPO NO2 para las fechas configuradas.
        """
        print(f"🚀 Descargando datos desde {self.start_date} hasta {self.end_date} ...")

        fetch_granule_data(
            start_date=self.start_date,
            end_date=self.end_date,
            folder=self.folder,
            download_list=self.download_list,
            download_script_template=self.download_script_template,
            download_script=self.download_script,
            skip_download=skip_download,
            verbose=verbose,
            dry_run=dry_run,
            only_one_file=only_one_file
        )

        print(f"✅ Data de TEMPO descargada en: {self.folder}")

if __name__ == "main":
    earthdata = EarthDataNO2()
    earthdata.download_data()