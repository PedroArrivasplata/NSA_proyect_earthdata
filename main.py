from get_data_tempo import get_data_tempo_today
from convert_nc_to_parquet import process_tempo_data
# Primero descargamos la data de hoy
# Teniendo en cuenta lo siguiente:
# CONCEPTS_ID
# C2930725014-LARC_CLOUD
# C3685896708-LARC_CLOUD

# data_tempo_today = get_data_tempo_today()
conversion_parquet = process_tempo_data(
    folder_nc="./tempo_data",
    folder_parquet="./tempo_parquet",
    features_to_keep=[
        "vertical_column_troposphere",
        "vertical_column_troposphere_uncertainty",
        "vertical_column_stratosphere",
        "main_data_quality_flag",
    ],
    group_data_name="product"
)


