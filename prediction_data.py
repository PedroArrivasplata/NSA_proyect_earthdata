from pathlib import Path
import pandas as pd
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score

root_dir = Path("./tempo_parquet")

today_str = datetime.utcnow().strftime("%Y-%m-%d")

file_path = root_dir / f"tempo_data_{today_str}.parquet"
df = pd.read_parquet(file_path)

features = [
    "vertical_column_troposphere",
    "vertical_column_troposphere_uncertainty",
    "vertical_column_stratosphere",
]

X = df[features]
y = df["main_data_quality_flag"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


