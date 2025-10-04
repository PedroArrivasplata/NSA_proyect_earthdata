import os
from pathlib import Path
import xarray as xr
import pandas as pd
import numpy as np
from tqdm import tqdm  # barra de progreso opcional

def process_tempo_data(folder_nc="./tempo_data", folder_parquet="./tempo_parquet"):
    """
    Procesa archivos .nc de TEMPO NO2 y los convierte en Parquet de forma optimizada.
    Ignora archivos vacíos o sin dimensiones válidas.
    """

    folder_parquet = Path(folder_parquet)
    folder_parquet.mkdir(parents=True, exist_ok=True)

    rutas_nc = [str(p) for p in Path(folder_nc).rglob("*.nc")]
    if not rutas_nc:
        print("⚠️ No se encontraron archivos .nc.")
        return None

    print(f"📂 Archivos encontrados: {len(rutas_nc)}")

    all_dataframes = []
    total_files = 0

    for ruta in tqdm(rutas_nc, desc="Procesando archivos"):
        try:
            ds = xr.open_dataset(ruta, engine="h5netcdf", group="product")

            # Verificar dimensiones válidas
            if not ds.dims:
                print(f"⚠️ Archivo sin dimensiones válidas: {ruta}")
                ds.close()
                continue

            # Variables a extraer
            vars_to_keep = [
                "vertical_column_troposphere",
                "vertical_column_troposphere_uncertainty",
                "vertical_column_stratosphere",
                "main_data_quality_flag",
            ]
            available_vars = [v for v in vars_to_keep if v in ds.variables]

            if not available_vars:
                print(f"⚠️ Archivo sin variables requeridas: {ruta}")
                ds.close()
                continue

            # Convertir a DataFrame
            df = ds[available_vars].to_dataframe().reset_index()
            ds.close()

            # Limpiar
            df.replace([np.inf, -np.inf], np.nan, inplace=True)
            df.dropna(inplace=True)
            df["file_source"] = os.path.basename(ruta)

            if len(df) > 0:
                all_dataframes.append(df)
                total_files += 1

        except Exception as e:
            print(f"❌ Error en {ruta}: {e}")

    if not all_dataframes:
        print("⚠️ No se generaron DataFrames válidos.")
        return None

    # Combinar todos los DataFrames
    df_final = pd.concat(all_dataframes, ignore_index=True)
    print(f"✅ Data combinada con {len(df_final):,} registros de {total_files} archivos válidos.")

    # Guardar en parquet
    output_path = folder_parquet / f"tempo_data_{pd.Timestamp.now().date()}.parquet"
    df_final.to_parquet(output_path, compression="snappy", engine="pyarrow", index=False)

    print(f"💾 Archivo Parquet guardado en: {output_path}")
    return output_path


if __name__ == "__main__":
    output_file = process_tempo_data("./tempo_data", "./tempo_parquet")
    if output_file:
        df = pd.read_parquet(output_file)
        print("🔍 Vista previa:")
        print(df.head())