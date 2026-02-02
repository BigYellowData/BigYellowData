import os
from pathlib import Path
import joblib
import pandas as pd
from sklearn.metrics import mean_squared_error
from sklearn.base import BaseEstimator

MODEL_NAME = "taxi_price_model"

# Determine paths relative to this file's location (ex05_ml_prediction_service)
_module_dir = Path(__file__).resolve().parent.parent
MODEL_SAVE_PATH = str(_module_dir / "models" / f"{MODEL_NAME}.joblib")
MODELS_DIR = str(_module_dir / "models")
REQUIRED_COLUMNS = [
    'trip_distance',
    'pickup_location_id',
    'dropoff_location_id',
    'tpep_pickup_datetime'
]
TARGET_COLUMN = 'total_amount'


def test_model(
        model: BaseEstimator,
        X_test: pd.DataFrame,
        y_test: pd.Series
) -> float:
    """
    Evaluate the model performance on the test set.

    Parameters
    ----------
    model : sklearn.base.BaseEstimator
        The trained model to evaluate.
    X_test : pd.DataFrame
        The feature matrix for testing.
    y_test : pd.Series
        The ground truth target variable.

    Returns
    -------
    float
        The Root Mean Squared Error (RMSE) calculated on the test set.
    """
    print(f"Evaluating model on {len(X_test)} rows...")
    predictions = model.predict(X_test)
    rmse = mean_squared_error(y_test, predictions, squared=False)

    return float(rmse)


def save_model(model: BaseEstimator):
    """
    Persist the model to disk.

    Parameters
    ----------
    model : sklearn.base.BaseEstimator
        The trained model object to be serialized.
    """
    os.makedirs(MODELS_DIR, exist_ok=True)
    joblib.dump(model, MODEL_SAVE_PATH)
    print(f"Model saved to {MODEL_SAVE_PATH}")
