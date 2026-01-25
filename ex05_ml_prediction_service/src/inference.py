import joblib
import pandas as pd
import os
from data_manager import prepare_data

MODEL_NAME = "taxi_price_model"
MODEL_LOAD_PATH = f"models/{MODEL_NAME}.joblib"


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
        The loaded Scikit-Learn model ready for prediction.

    Raises
    ------
    FileNotFoundError
        If the model file does not exist at the specified path.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Error model not found at {path}")
    print("Loading model...")
    return joblib.load(path)


def predict_trip_price(model, trip_data: pd.DataFrame) -> float:
    """
    Generate a price prediction for a given trip.

    This function acts as a wrapper around the model's predict method.
    It handles the necessary data formatting to match the training schema.

    Note
    ----
    This function injects a dummy 'total_amount' column set to 0.
    This is required because the shared `prepare_data` function expects
    this column to exist, even though it is not used for inference.

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
        The predicted price of the trip.

    Raises
    ------
    ValueError
        If data preparation fails (e.g. missing columns in input).
    """
    trip_data = trip_data.copy()
    # Injection of dummy target to satisfy prepare_data requirements
    trip_data['total_amount'] = 0

    try:
        X, _ = prepare_data(trip_data)
    except Exception as e:
        raise ValueError(f"Data Preparation Failed: {e}")

    price = model.predict(X)[0]

    return price