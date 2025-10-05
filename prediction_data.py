from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import r2_score, mean_squared_error
from joblib import dump
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout

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

scaler_X = MinMaxScaler()
scaler_y = MinMaxScaler()

X_scaled = scaler_X.fit_transform(X)
y_scaled = scaler_y.fit_transform(y.values.reshape(-1, 1))

def create_sequences(X, y, seq_length=10):
    Xs, ys = [], []
    for i in range(len(X) - seq_length):
        Xs.append(X[i:i + seq_length])
        ys.append(y[i + seq_length])
    return np.array(Xs), np.array(ys)

SEQ_LENGTH = 10
X_seq, y_seq = create_sequences(X_scaled, y_scaled, SEQ_LENGTH)

split = int(0.8 * len(X_seq))
X_train, X_test = X_seq[:split], X_seq[split:]
y_train, y_test = y_seq[:split], y_seq[split:]

print(f"🧩 Datos para entrenamiento: {X_train.shape}, prueba: {X_test.shape}")

model = Sequential([
    LSTM(64, return_sequences=True, input_shape=(SEQ_LENGTH, len(features))),
    Dropout(0.2),
    LSTM(32, return_sequences=False),
    Dense(16, activation="relu"),
    Dense(1)
])

model.compile(optimizer="adam", loss="mse")
model.summary()

history = model.fit(
    X_train, y_train,
    validation_data=(X_test, y_test),
    epochs=20,
    batch_size=64,
    verbose=1
)

pred_scaled = model.predict(X_test)
pred = scaler_y.inverse_transform(pred_scaled)
y_real = scaler_y.inverse_transform(y_test)

rmse = np.sqrt(mean_squared_error(y_real, pred))
r2 = r2_score(y_real, pred)
print(f"✅ RMSE: {rmse:.4f}")
print(f"✅ R²: {r2:.4f}")


model.save("tempo_forecast_model.h5")
dump(model, "tempo_forecast_model.joblib")
dump(scaler_X, "scaler_X.joblib")
dump(scaler_y, "scaler_y.joblib")

print("💾 Modelo y escaladores guardados correctamente.")