import pytest
import pandas as pd
import numpy as np
from src.data_manager import prepare_data

@pytest.fixture
def valid_data():
    return pd.DataFrame({
        'trip_distance': [1.5, 3.2, 0.5],
        'PULocationID': [100, 101, 102],
        'DOLocationID': [200, 201, 202],
        'total_amount': [15.0, 30.5, 10.0],
        'tpep_pickup_datetime': pd.to_datetime(['2024-01-01 10:00', '2024-01-01 11:00', '2024-01-01 12:00'])
    })

def test_prepare_data_output_shape(valid_data):
    """Vérifie que la fonction retourne bien X et y avec les bonnes dimensions"""
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
def test_missing_required_columns(valid_data, missing_col):
    broken_df = valid_data.drop(columns=[missing_col])
    with pytest.raises(KeyError):
        prepare_data(broken_df)


def test_negative_value_handling(valid_data):
    bad_distance = valid_data.copy()
    bad_distance.loc[0, 'trip_distance'] = -1.0
    with pytest.raises(ValueError):
        prepare_data(bad_distance)

    bad_price = valid_data.copy()
    bad_price.loc[0, 'total_amount'] = -1.0
    with pytest.raises(ValueError):
        prepare_data(bad_price)