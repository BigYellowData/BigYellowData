import pytest
import pandas as pd
from src.model_manager import train_model
from src.data_manager import prepare_data


@pytest.fixture
def train_sample():
    """
    Fixture providing a small synthetic dataset for training tests.

    Returns
    -------
    pd.DataFrame
        A DataFrame with 3 rows containing all necessary features
        (distance, locations, dates) and the target (total_amount).
    """
    # TODO: remplacer ces données par quelques vrais entrées du dataset
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


def test_coefficient(train_sample: pd.DataFrame):
    """
    Verify the Business Logic.

    This test inspects the internal weights of the linear model to ensure
    it learns that distance is positively correlated with price.

    Parameters
    ----------
    train_sample : pd.DataFrame
        The fixture data.
    """
    X_train, y_train = prepare_data(train_sample)
    model = train_model(X_train, y_train)

    # We check the coefficient of the first feature (trip_distance)
    distance_coef = model.coef_[0]

    assert distance_coef > 0, (
        "ERROR: trip_distance has to increase total_amount"
    )


def test_learn(train_sample: pd.DataFrame):
    """
    Verify the mathematical convergence (Sanity Check).

    This test ensures that the Ordinary Least Squares (OLS) algorithm
    is working correctly. Mathematically, for a Linear Regression with
    an intercept, the sum (and mean) of residuals on the training set
    must be zero (or extremely close to it due to floating point precision).

    Parameters
    ----------
    train_sample : pd.DataFrame
        The fixture data.
    """
    X_train, y_train = prepare_data(train_sample)
    model = train_model(X_train, y_train)
    predictions = model.predict(X_train)
    mean_error = (predictions - y_train).mean()

    assert abs(mean_error) < 1e-4, (
        "ERROR: biased model can not learn (Mean Residuals != 0)"
    )