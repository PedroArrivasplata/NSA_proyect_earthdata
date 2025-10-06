# (opcional) crear venv y activarlo
python -m venv venv
source venv/bin/activate          
### Windows: venv\Scripts\activate

pip install -r requirements.txt

## (opcional) crear artifacts y generar el modelo
python3 -m scripts.train_fake_model

## (opcional) eliminar .joblib viejos para evitar confusiones
find . -name "*.joblib" -not -path "./artifacts/*" -print -delete


# levantar la API (usando ese MODEL_PATH por defecto)
python3 -m uvicorn main:app --reload

API: http://127.0.0.1:8000/api/predict
