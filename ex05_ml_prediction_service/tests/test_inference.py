import pytest
import pandas as pd
from src.inference import load_model, predict_trip_price
from src.model_manager import MODEL_SAVE_PATH


def test_load_model():
    """
    Verify that the trained model artifact exists and is loadable.

    This test acts as a 'Smoke Test' for deployment. If it fails,
    the application cannot start.

    Scenario:
        Attempt to load the model from the defined constant MODEL_SAVE_PATH.
    Expected Result:
        The function should execute without raising FileNotFoundError.
    """
    try:
        load_model(MODEL_SAVE_PATH)
    except FileNotFoundError:
        pytest.fail(
            "ERROR: model not found at saving path. Did you run train.py?"
        )


def test_predict_price():
    """
    Verify the end-to-end prediction capability.

    This test ensures that the loaded model object is compatible with
    the input data schema constructed manually.

    Scenario:
        1. Load the real model from disk.
        2. Create a single-row DataFrame with raw inputs (strings, ints).
        3. Call the prediction wrapper.
    Expected Result:
        The model returns a float value strictly greater than 0
        (sanity check for a price).
    """
    model = load_model(MODEL_SAVE_PATH)

    trip_test = pd.DataFrame({
        'trip_distance': [1.5],
        'PULocationID': [100],
        'DOLocationID': [200],
        'tpep_pickup_datetime': ['2024-01-01 10:00']
    })

    amount = predict_trip_price(model, trip_test)
    assert amount > 0, f"ERROR: model predict negative prices: {amount}"