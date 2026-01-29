import pytest
import pandas as pd
from src.inference import load_model, predict_trip_price
from src.model_manager import MODEL_SAVE_PATH, REQUIRED_COLUMNS, TARGET_COLUMN
from src.data_manager import prepare_data_inference


@pytest.fixture
def training_data():
    """
    Fixture providing a valid sample DataFrame meant for training a model.

    This DataFrame contains all required columns with valid data types
    and realistic values, serving as a baseline for positive tests.

    Returns
    -------
    pd.DataFrame
        A 3-row DataFrame mimicking the raw taxi dataset.
    """
    return pd.DataFrame({
        'trip_distance': [1.5, 3.2, 0.5],
        'PULocationID': [100, 101, 102],
        'DOLocationID': [200, 201, 202],
        'total_amount': [15.0, 30.5, 10.0],
        'tpep_pickup_datetime': pd.to_datetime([
            '2024-01-01 10:00',
            '2024-01-01 11:00',
            '2024-01-01 12:00'
        ])
    })


def test_column(training_data: pd.DataFrame):
    """
    Verify strict schema compliance for inference data.

    This test ensures that the data preparation function for inference
    enforces a strict schema:
    1. It rejects data containing the target column (to avoid leakage).
    2. It requires all feature columns to be present.

    Parameters
    ----------
    training_data : pd.DataFrame
        The valid training dataframe fixture (containing the target).

    Raises
    ------
    KeyError
        If the target is present or if a required feature is missing.
    """
    # Case 1: Presence of target column should raise error (Strict Schema)
    with pytest.raises(KeyError):
        prepare_data_inference(training_data)

    # Case 2: Missing any required column for inference
    inference_data = training_data.drop(columns=[TARGET_COLUMN])
    for col in REQUIRED_COLUMNS:
        incomplete_df = inference_data.copy().drop(columns=[col])
        with pytest.raises(KeyError):
            prepare_data_inference(incomplete_df)


def test_load_model():
    """
    Verify that the trained model artifact exists and is loadable.

    This test acts as a 'Smoke Test' for deployment. If it fails,
    the application cannot start.

    Scenario
    --------
    Attempt to load the model from the defined constant MODEL_SAVE_PATH.

    Raises
    ------
    pytest.fail
        If the model file is not found (FileNotFoundError).
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

    Scenario
    --------
    1. Load the real model from disk.
    2. Create a single-row DataFrame with raw inputs.
    3. Call the prediction wrapper.

    Raises
    ------
    AssertionError
        If the prediction is not a float or is not strictly positive.
    """
    model = load_model(MODEL_SAVE_PATH)

    trip_test = pd.DataFrame({
        'trip_distance': [1.5],
        'PULocationID': [100],
        'DOLocationID': [200],
        'tpep_pickup_datetime': ['2024-01-01 10:00']
    })

    amount = predict_trip_price(model, trip_test)

    assert isinstance(amount, float), "Prediction output must be a float"
    assert amount > 0, f"ERROR: model predict negative prices: {amount}"
