import os
from pathlib import Path
import xarray as xr
import pandas as pd
import numpy as np

def compile_tempo_data(folder="./tempo_data"):
    rutas_nc = []

    # Buscar todos los archivos .nc en la carpeta (incluyendo subcarpetas)
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith(".nc"):
                rutas_nc.append(os.path.join(root, file))

    if not rutas_nc:
        print("⚠️ No se encontraron archivos .nc en la carpeta especificada.")
        return pd.DataFrame()

    registros = []  # Aquí guardaremos los diccionarios que luego serán filas del DataFrame

    for ruta in rutas_nc:
        try:
            # === Datos del producto ===
            ds_product = xr.open_dataset(ruta, group="product")
            var_tropo = ds_product['vertical_column_troposphere'].values.flatten()
            var_tropo_uncertain = ds_product['vertical_column_troposphere_uncertainty'].values.flatten()
            var_strato = ds_product['vertical_column_stratosphere'].values.flatten()
            var_quality_flag = ds_product['main_data_quality_flag'].values.flatten()



            # === Validar que todas las variables tengan la misma longitud ===
            min_len = min(
                len(var_tropo), len(var_tropo_uncertain),
                len(var_strato), len(var_quality_flag)
            )

            # === Crear registros limpios ===
            for i in range(min_len):
                registros.append({
  
                    "vertical_column_troposphere": var_tropo[i],
                    "vertical_column_troposphere_uncertainty": var_tropo_uncertain[i],
                    "vertical_column_stratosphere": var_strato[i],
                    "main_data_quality_flag": var_quality_flag[i],
                    "file_source": os.path.basename(ruta)
                })

            # Cerrar datasets para liberar memoria
            ds_product.close()

        except Exception as e:
            print(f"❌ Error procesando {ruta}: {e}")

    # Convertir a DataFrame
    df = pd.DataFrame(registros)

    # Eliminar filas con NaN o valores inválidos
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.dropna(inplace=True)

    print(f"✅ Data compilada correctamente con {len(df)} registros de {len(rutas_nc)} archivos.")
    return df


# === Ejemplo de uso ===
if __name__ == "__main__":
    df_tempo = compile_tempo_data("./tempo_data")
    print(df_tempo.head())