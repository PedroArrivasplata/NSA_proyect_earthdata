from get_data_tempo import get_data_tempo_today
from convert_nc_to_parquet import process_tempo_data
# Primero descargamos la data de hoy
# Teniendo en cuenta lo siguiente:
# CONCEPTS_ID
# C2930725014-LARC_CLOUD
# C3685896708-LARC_CLOUD

data_tempo_today = get_data_tempo_today()
conversion_parquet = process_tempo_data()