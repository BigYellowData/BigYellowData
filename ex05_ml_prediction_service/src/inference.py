import joblib
import pandas as pd
import os
import sys
from data_manager import prepare_data

MODEL_NAME = "taxi_price_model"
MODEL_PATH = f"models/{MODEL_NAME}.joblib"

def load_model(path: str = MODEL_PATH):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Error model not found at {path}")
    print("Loading model...")
    return joblib.load(path)

def predict_trip_price(model, trip_data: pd.DataFrame) -> float:
    trip_data = trip_data.copy()
    trip_data['total_amount'] = 0
    try:
        X, _ = prepare_data(trip_data)
    except Exception as e:
        raise ValueError(f"Data Preparation Failed: {e}")
    price = model.predict(X)[0]

    return price