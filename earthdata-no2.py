from pathlib import Path
from datetime import datetime, timezone
from data_tempo_utils import (
    fetch_granule_data,
    setup_data_folder
)

class EarthDataNO2:
    def __init__(self, root_dir: str = "./tempo_data", template_script: str = "./download_template.sh"):
        """
        Clase para descargar datos TEMPO NO2 de Earthdata (NASA).
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

    def download_data_today(self):
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
            skip_download=False,
            verbose=False,
            dry_run=False,
            only_one_file=False
        )

        print(f"✅ Data de TEMPO descargada en: {self.folder}")

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

        print(f"📅 Descargando datos desde {date_start} hasta {date_end}")

        fetch_granule_data(
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

        print(f"✅ Data de TEMPO descargada en: {self.folder}")

if __name__ == "__main__":
    earthdata = EarthDataNO2()
