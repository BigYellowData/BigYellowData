import pytest
import pandas as pd
from src.data_manager import prepare_data_training
from src.model_manager import REQUIRED_COLUMNS, TARGET_COLUMN


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
        'pickup_location_id': [100, 101, 102],
        'dropoff_location_id': [200, 201, 202],
        'total_amount': [15.0, 30.5, 10.0],
        'tpep_pickup_datetime': pd.to_datetime([
            '2024-01-01 10:00',
            '2024-01-01 11:00',
            '2024-01-01 12:00'
        ])
    })


@pytest.fixture
def broken_df_distance():
    """
    Fixture providing a broken sample DataFrame for testing.

    This DataFrame contains all required columns with valid data types
    and realistic values, except for trip_distance which contains
    invalid values (zero).

    Returns
    -------
    pd.DataFrame
        A 3-row DataFrame mimicking the raw taxi dataset with
        invalid distances.
    """
    return pd.DataFrame({
        'trip_distance': [0, 3.2, 0.5],
        'pickup_location_id': [100, 101, 102],
        'dropoff_location_id': [200, 201, 202],
        'total_amount': [15.0, 30.5, 10.0],
        'tpep_pickup_datetime': pd.to_datetime([
            '2024-01-01 10:00',
            '2024-01-01 11:00',
            '2024-01-01 12:00'
        ])
    })


@pytest.fixture
def broken_df_amount():
    """
    Fixture providing a broken sample DataFrame for testing.

    This DataFrame contains all required columns with valid data types
    and realistic values, except for total_amount which contains
    negative values.

    Returns
    -------
    pd.DataFrame
        A 3-row DataFrame mimicking the raw taxi dataset with
        invalid prices.
    """
    return pd.DataFrame({
        'trip_distance': [1.5, 3.2, 0.5],
        'pickup_location_id': [100, 101, 102],
        'dropoff_location_id': [200, 201, 202],
        'total_amount': [-1.0, 30.5, 10.0],
        'tpep_pickup_datetime': pd.to_datetime([
            '2024-01-01 10:00',
            '2024-01-01 11:00',
            '2024-01-01 12:00'
        ])
    })


def test_column(valid_data: pd.DataFrame):
    """
    Verify that missing required columns raise a KeyError.

    This test checks the robustness of the data preparation function
    against schema violations. It ensures that both the target column
    and feature columns are strictly required.

    Parameters
    ----------
    valid_data : pd.DataFrame
        The valid data fixture to be corrupted by dropping columns.

    Raises
    ------
    KeyError
        If a required column is missing during data preparation.
    """
    # Case 1: Missing target column
    incomplete_df = valid_data.copy().drop(columns=[TARGET_COLUMN])
    with pytest.raises(KeyError):
        prepare_data_training(incomplete_df)

    # Case 2: Missing any required column for training
    for col in REQUIRED_COLUMNS:
        incomplete_df = valid_data.copy().drop(columns=[col])
        with pytest.raises(KeyError):
            prepare_data_training(incomplete_df)


def test_values(
        broken_df_distance: pd.DataFrame,
        broken_df_amount: pd.DataFrame
):
    """
    Verify that invalid data values raise a ValueError.

    This test checks the data validation logic (business rules),
    ensuring that distances are strictly positive and amounts are
    non-negative.

    Parameters
    ----------
    broken_df_distance : pd.DataFrame
        Fixture with invalid distance values (e.g., 0).
    broken_df_amount : pd.DataFrame
        Fixture with invalid amount values (e.g., negative).

    Raises
    ------
    ValueError
        If data values violate business rules.
    """
    # Case 1: Invalid distance
    with pytest.raises(ValueError):
        prepare_data_training(broken_df_distance)

    # Case 2: Invalid amount
    with pytest.raises(ValueError):
        prepare_data_training(broken_df_amount)


def test_output_shape(valid_data: pd.DataFrame):
    """
    Verify the consistency of the output dimensions.

    This test ensures that the data splitting and transformation
    process preserves the number of samples and correctly separates
    features (X) from the target (y).

    Parameters
    ----------
    valid_data : pd.DataFrame
        The valid data fixture.
    """
    X_train, y_train = prepare_data_training(valid_data)

    # Check that row counts match input length
    assert len(X_train) == len(valid_data) == len(y_train)

    # Check that target is removed from features
    assert TARGET_COLUMN not in X_train.columns

    # Check that target series matches input values
    assert y_train.equals(valid_data[TARGET_COLUMN])
