import os
from pathlib import Path
import joblib
import pandas as pd
from data_manager import prepare_data_inference

MODEL_NAME = "taxi_price_model"

# Determine the model path relative to this file's location
_module_dir = Path(__file__).resolve().parent.parent
MODEL_LOAD_PATH = str(_module_dir / "models" / f"{MODEL_NAME}.joblib")


def load_model(path: str = MODEL_LOAD_PATH):
    """
    Load the trained model from the disk.

    Parameters
    ----------
    path : str, optional
        The file path to the serialized model (.joblib).
        Defaults to the constant MODEL_LOAD_PATH defined in the module.

    Returns
    -------
    object
        The loaded model ready for prediction.

    Raises
    ------
    FileNotFoundError
        If the model file does not exist at the specified path.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Error: model not found at {path}")
    print(f"Loading model from {path}...")
    return joblib.load(path)


def predict_trip_price(model, trip_data: pd.DataFrame) -> float:
    """
    Generate a price prediction for a given trip.

    This function acts as a wrapper around the model's predict method.
    It handles the necessary data formatting using the inference
    preparation pipeline.

    Parameters
    ----------
    model : object
        The trained model object (must have a .predict() method).
    trip_data : pd.DataFrame
        Raw input data containing trip features (trip_distance,
        PULocationID, etc.).

    Returns
    -------
    float
        The predicted price of the trip. If multiple rows are provided,
        returns the prediction for the first row only.

    Raises
    ------
    ValueError
        If data preparation fails.
    """
    trip_data = trip_data.copy()

    try:
        X = prepare_data_inference(trip_data)
    except Exception as e:
        raise ValueError(f"Data Preparation Failed: {e}")

    # Takes the first prediction from the result array
    price = model.predict(X)[0]

    return float(price)
