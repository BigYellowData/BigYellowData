import os
import pandas as pd
import sys
import shutil
from model_manager import REQUIRED_COLUMNS, TARGET_COLUMN
from pathlib import Path
from minio import Minio
from sklearn.model_selection import train_test_split
from dotenv import load_dotenv

# Load .env file from project root
env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(env_path)

# Constants for MinIO connection (from environment or defaults)
# Use localhost:9000 for local execution (default), minio:9000 for Docker
_raw_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
# Replace Docker hostname 'minio' with 'localhost' for local execution
MINIO_ENDPOINT = _raw_endpoint.replace("http://", "").replace("https://", "").replace("minio:", "localhost:")
ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
BUCKET_NAME = "nyctaxiproject"
OBJECT_PREFIX = "dwh/yellow_taxi_refined"


def download_from_minio(dest_folder: str):
    """
    Download the dataset from the MinIO bucket to a local folder.

    It cleans the destination folder before downloading to ensure freshness.

    Parameters
    ----------
    dest_folder : str
        The local path where the files should be downloaded
        (e.g., "../data/processed").

    Raises
    ------
    FileNotFoundError
        If no files matching the prefix are found in the bucket.
    """
    client = Minio(
        MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False  # Important : False as we use local HTTP, no HTTPS
    )
    dest_path = Path(dest_folder)

    # Clean up existing directory
    if dest_path.exists():
        shutil.rmtree(dest_path)
    dest_path.mkdir(parents=True, exist_ok=True)

    print(f"Connecting on MinIO ({MINIO_ENDPOINT}) on '{BUCKET_NAME}'...")
    objects = client.list_objects(
        BUCKET_NAME,
        prefix=OBJECT_PREFIX,
        recursive=True
    )

    found_files = False
    for obj in objects:
        # Ignoring virtual directories and Spark success files
        if obj.object_name.endswith('/'):
            continue
        if "_SUCCESS" in obj.object_name:
            continue

        file_name = Path(obj.object_name).name
        local_file_path = dest_path / file_name
        print(f"    Downloading : {obj.object_name}")
        client.fget_object(
            BUCKET_NAME,
            obj.object_name,
            str(local_file_path)
        )
        found_files = True

    if not found_files:
        raise FileNotFoundError(
            f"Error no file found with prefix : {OBJECT_PREFIX}"
        )
    print("Done.")


def load_data(filepath: str, test_rate=0.2):
    """
    Orchestrate data loading: download, read, and split.

    This function attempts to download fresh data from MinIO, reads the
    Parquet file from the given path, and performs a train/test split.

    Parameters
    ----------
    filepath : str
        Path to the folder or file containing the parquet data.
    test_rate : float, optional
        Proportion of the dataset to include in the test split.

    Returns
    -------
    tuple of (pd.DataFrame, pd.DataFrame)
        - df_train: The training set.
        - df_test: The testing set.

    Raises
    ------
    FileNotFoundError
        If the specified filepath does not exist locally.
    """
    try:
        download_from_minio(filepath)
    except Exception as e:
        print(f"Error while downloading from MinIO server : {e}")

    path_obj = Path(filepath)
    if not path_obj.exists():
        raise FileNotFoundError(f"File not found at : {path_obj.resolve()}")

    try:
        df = pd.read_parquet(path_obj)
        df_train, df_test = train_test_split(
            df,
            test_size=test_rate,
            shuffle=True
        )
        print(f"Train: {df_train.shape} \nTest: {df_test.shape}\n")

        return df_train, df_test
    except Exception as e:
        print(f"Error while reading parquet files : {e}")
        sys.exit(1)


def prepare_data_training(df: pd.DataFrame):
    """
    Feature engineering and data cleaning pipeline for model training.

    It performs the following steps:
    1. Filters out outliers (if is_outlier column exists).
    2. Validates existence of required columns and target column.
    3. Checks for data anomalies (negative values).
    4. Extracts time-based features (hour, day of week).
    5. Creates derived features (is_rush_hour, is_weekend, is_airport).
    6. Separates features (X) from the target (y).

    Parameters
    ----------
    df : pd.DataFrame
        The raw input dataframe containing trip information.

    Returns
    -------
    tuple of (pd.DataFrame, pd.Series)
        - X (DataFrame): The feature matrix with engineered features.
        - y (Series): The target variable (total_amount).

    Raises
    ------
    KeyError
        If a required column is missing from the input dataframe.
    ValueError
        If negative values are detected in distance or amount.
    """
    df = df.copy()

    # Filter outliers if the column exists (from ex02 data)
    if 'is_outlier' in df.columns:
        initial_count = len(df)
        df = df[df['is_outlier'] == False]
        filtered_count = initial_count - len(df)
        print(f"Filtered outliers: {filtered_count} rows removed ({filtered_count/initial_count*100:.1f}%)")

    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            raise KeyError(f"Missing required column {col}")

    if TARGET_COLUMN not in df.columns:
        raise KeyError(f"Missing target column {TARGET_COLUMN}")

    if (df['trip_distance'] <= 0).any() or (df['total_amount'] < 0).any():
        raise ValueError("Error: negative value detected")

    if df['tpep_pickup_datetime'].dtype == 'object':
        df['tpep_pickup_datetime'] = pd.to_datetime(
            df['tpep_pickup_datetime']
        )

    # Basic time features
    df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour
    df['day_of_week'] = df['tpep_pickup_datetime'].dt.dayofweek

    # Derived features for better predictions
    df['is_rush_hour'] = df['pickup_hour'].isin([7, 8, 9, 17, 18, 19]).astype(int)
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
    df['is_night'] = df['pickup_hour'].isin([22, 23, 0, 1, 2, 3, 4, 5]).astype(int)

    # Airport detection from service zones (if available from ex02)
    if 'PU_service_zone' in df.columns and 'DO_service_zone' in df.columns:
        df['is_airport_trip'] = (
            (df['PU_service_zone'] == 'Airports') |
            (df['DO_service_zone'] == 'Airports')
        ).astype(int)
    else:
        # Fallback: use known airport location IDs (JFK=132, LaGuardia=138, Newark=1)
        airport_ids = [1, 132, 138]
        df['is_airport_trip'] = (
            df['pickup_location_id'].isin(airport_ids) |
            df['dropoff_location_id'].isin(airport_ids)
        ).astype(int)

    features = [
        'trip_distance',
        'pickup_location_id',
        'dropoff_location_id',
        'pickup_hour',
        'day_of_week',
        'is_rush_hour',
        'is_weekend',
        'is_night',
        'is_airport_trip'
    ]
    target = 'total_amount'

    return df[features], df[target]


def prepare_data_inference(df: pd.DataFrame):
    """
    Feature engineering and data cleaning pipeline for inference.

    It performs the following steps:
    1. Validates existence of required columns and absence of target column.
    2. Checks for data anomalies (negative values).
    3. Extracts time-based features (hour, day of week).
    4. Creates derived features (is_rush_hour, is_weekend, is_airport).

    Parameters
    ----------
    df : pd.DataFrame
        The raw input dataframe containing trip information.

    Returns
    -------
    X (DataFrame): The feature matrix with engineered features.

    Raises
    ------
    KeyError
        If a required column is missing or target column
        is present from the input dataframe.
    ValueError
        If negative value is detected in distance.
    """
    df = df.copy()

    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            raise KeyError(f"Missing required column {col}")

    if TARGET_COLUMN in df.columns:
        raise KeyError(f"Target column {TARGET_COLUMN} should "
                       f"not be known for inference")

    if (df['trip_distance'] <= 0).any():
        raise ValueError("Error: negative value detected")

    if df['tpep_pickup_datetime'].dtype == 'object':
        df['tpep_pickup_datetime'] = pd.to_datetime(
            df['tpep_pickup_datetime']
        )

    # Basic time features
    df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour
    df['day_of_week'] = df['tpep_pickup_datetime'].dt.dayofweek

    # Derived features (must match training)
    df['is_rush_hour'] = df['pickup_hour'].isin([7, 8, 9, 17, 18, 19]).astype(int)
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
    df['is_night'] = df['pickup_hour'].isin([22, 23, 0, 1, 2, 3, 4, 5]).astype(int)

    # Airport detection (use known airport location IDs)
    airport_ids = [1, 132, 138]
    df['is_airport_trip'] = (
        df['pickup_location_id'].isin(airport_ids) |
        df['dropoff_location_id'].isin(airport_ids)
    ).astype(int)

    features = [
        'trip_distance',
        'pickup_location_id',
        'dropoff_location_id',
        'pickup_hour',
        'day_of_week',
        'is_rush_hour',
        'is_weekend',
        'is_night',
        'is_airport_trip'
    ]

    return df[features]
