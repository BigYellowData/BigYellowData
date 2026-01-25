import pytest
import pandas as pd
from src.data_manager import prepare_data

@pytest.fixture
def valid_data():
    """
    Fixture providing a valid sample DataFrame for testing.
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
        'tpep_pickup_datetime': pd.to_datetime(['2024-01-01 10:00', '2024-01-01 11:00', '2024-01-01 12:00'])
    })

def test_prepare_data_output_shape(valid_data: pd.DataFrame):
    """
    Verify that the data preparation returns correctly shaped X and y.
    Scenario:
        Input is a perfectly valid DataFrame.
    Expected Result:
        1. X (features) should not contain the target 'total_amount'.
        2. X and y should have the same number of rows.
        3. y should match the input 'total_amount' column exactly.

    Parameters
    ----------
    valid_data : pd.DataFrame
        The fixture data.
    """
    X, y = prepare_data(valid_data)
    assert 'total_amount' not in X.columns
    assert len(X) == len(y)
    assert y.equals(valid_data['total_amount'])

@pytest.mark.parametrize("missing_col", [
    'trip_distance',
    'PULocationID',
    'DOLocationID',
    'total_amount',
    'tpep_pickup_datetime'
])
def test_missing_required_columns(valid_data: pd.DataFrame, missing_col: str):
    """
    Verify schema validation for missing columns.
    This test iterates over each required column and removes it
    to ensure the function raises a KeyError.

    Parameters
    ----------
    valid_data : pd.DataFrame
        The fixture data.
    missing_col : str
        The name of the column to drop for this specific test iteration
        (injected by pytest.mark.parametrize).
    """
    broken_df = valid_data.drop(columns=[missing_col])
    with pytest.raises(KeyError):
        prepare_data(broken_df)

def test_negative_value_handling(valid_data: pd.DataFrame):
    """
    Verify business logic validation for negative values.
    Scenario:
        Input contains negative distance or negative price.
    Expected Result:
        The function should raise a ValueError to prevent training on corrupted data.

    Parameters
    ----------
    valid_data : pd.DataFrame
        The fixture data.
    """
    # Case 1: Negative Distance
    bad_distance = valid_data.copy()
    bad_distance.loc[0, 'trip_distance'] = -1.0
    with pytest.raises(ValueError):
        prepare_data(bad_distance)
    # Case 2: Negative Price
    bad_price = valid_data.copy()
    bad_price.loc[0, 'total_amount'] = -1.0
    with pytest.raises(ValueError):
        prepare_data(bad_price)