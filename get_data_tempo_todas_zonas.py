from pathlib import Path
from datetime import datetime
import subprocess

# Función para generar lista de zonas
def generate_zone_files(base_pattern: str, date: str, version="V04", product="TEMPO_NO2_L3"):
    """
    Genera los nombres de ficheros desde zona 1 hasta 7
    Ejemplo: TEMPO_NO2_L3_V04_20251004T125055Z_S001.nc
    """
    zone_files = []
    for zone in range(1, 8):  # Zonas 1 → 7
        zone_id = f"S00{zone}"
        file_name = f"{product}_{version}_{date}_{zone_id}.nc"
        zone_files.append(file_name)
    return zone_files


def download_data(date: str, folder: Path):
    """
    Descarga datos TEMPO para todas las zonas (S001-S007)
    """
    nasa_base_url = "https://data.asdc.earthdata.nasa.gov/asdc-prod-protected/TEMPO/TEMPO_NO2_L3_V04"
    
    # Generar lista de ficheros para todas las zonas
    zone_files = generate_zone_files(base_pattern="TEMPO_NO2_L3", date=date)
    
    for file_name in zone_files:
        url = f"{nasa_base_url}/{date[:4]}.{date[4:6]}.{date[6:8]}/{file_name}"
        output_file = folder / file_name
        
        print(f"⬇️ Descargando: {url}")
        
        try:
            subprocess.run(
                ["curl", "-f", "-n", "-c", "cookies.txt", "-b", "cookies.txt", "-L", url, "-o", str(output_file)],
                check=True
            )
            print(f"✅ Guardado en: {output_file}")
        except subprocess.CalledProcessError:
            print(f"❌ No se pudo descargar: {url}")


if __name__ == "__main__":
    # Fecha de hoy en formato YYYYMMDDTHHMMSSZ (como en los ficheros)
    today = datetime.utcnow()
    date_str = today.strftime("%Y%m%dT%H%M%SZ")
    
    # Carpeta destino
    root_dir = Path("./tempo_data").resolve()
    root_dir.mkdir(parents=True, exist_ok=True)
    
    # Descargar datos de todas las zonas
    download_data(date_str, root_dir)