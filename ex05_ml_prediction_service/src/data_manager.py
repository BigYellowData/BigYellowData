import pandas as pd
import sys
import shutil
from pathlib import Path
from minio import Minio
from sklearn.model_selection import train_test_split

# Constants for MinIO connection
MINIO_ENDPOINT = "localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "nyctaxiproject"
OBJECT_PREFIX = "nyc_clean_for_ML"

def download_from_minio(dest_folder: str):
    """
    Download the dataset from the MinIO bucket to a local folder.
    It cleans the destination folder before downloading to ensure freshness.

    Parameters
    ----------
    dest_folder : str
        The local path where the files should be downloaded (e.g., "../data/processed").

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

    print(f"Connecting on MinIO ({MINIO_ENDPOINT}) on '{BUCKET_NAME}' bucket...")
    objects = client.list_objects(BUCKET_NAME, prefix=OBJECT_PREFIX, recursive=True)

    found_files = False
    for obj in objects:
        # Ignoring virtual directories and Spark success files
        if obj.object_name.endswith('/'): continue
        if "_SUCCESS" in obj.object_name: continue

        file_name = Path(obj.object_name).name
        local_file_path = dest_path / file_name
        print(f"    Downloading : {obj.object_name}")
        client.fget_object(BUCKET_NAME, obj.object_name, str(local_file_path))
        found_files = True

    if not found_files:
        raise FileNotFoundError(f"Error no file found with prefix : {OBJECT_PREFIX}")
    print("Done.")

def prepare_data(df: pd.DataFrame):
    """
    Feature engineering and data cleaning pipeline.
    It performs the following steps:
    1. Validates existence of required columns.
    2. Checks for data anomalies (negative values).
    3. Extracts time-based features (hour, day of week).
    4. Separates features (X) from the target (y).

    Parameters
    ----------
    df : pd.DataFrame
        The raw input dataframe containing trip information.

    Returns
    -------
    tuple of (pd.DataFrame, pd.Series)
        - X (DataFrame): The feature matrix (distance, location IDs, time features).
        - y (Series): The target variable (total_amount).

    Raises
    ------
    KeyError
        If a required column is missing from the input dataframe.
    ValueError
        If negative values are detected in distance or amount.
    """
    # TODO: utilise du one-hot-encoder pour les locations ID ou les remplacer par un booléen to_airport
    required_cols = ['trip_distance', 'PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'total_amount']
    for col in required_cols:
        if col not in df.columns:
            raise KeyError(f"Missing required column {col}")

    if (df['trip_distance'] < 0).any() or (df['total_amount'] < 0).any():
        raise ValueError(f"Error: negative value detected")

    if df['tpep_pickup_datetime'].dtype == 'object':
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])

    df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour
    df['day_of_week'] = df['tpep_pickup_datetime'].dt.dayofweek

    features = ['trip_distance', 'PULocationID', 'DOLocationID', 'pickup_hour', 'day_of_week']
    target = 'total_amount'

    return df[features], df[target]

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
        Proportion of the dataset to include in the test split (default is 0.2).

    Returns
    -------
    tuple of (pd.DataFrame, pd.DataFrame)
        - df_train: The training set.
        - df_test: The testing set.

    Raises
    ------
    FileNotFoundError
        If the specified filepath does not exist locally after download attempt.
    """
    try:
        download_from_minio("../data/processed")
    except Exception as e:
        print(f"Error while downloading from MinIO server : {e}")

    path_obj = Path(filepath)
    if not path_obj.exists():
        raise FileNotFoundError(f"File not found at : {path_obj.resolve()}")

    try:
        df = pd.read_parquet(path_obj)
        df_train, df_test = train_test_split(df, test_size=test_rate, shuffle=True)
        print(f"Train: {df_train.shape} \nTest: {df_test.shape}\n")

        return df_train, df_test
    except Exception as e:
        print(f"Error while reading parquet files : {e}")
        sys.exit(1)