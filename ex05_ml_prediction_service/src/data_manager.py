import pandas as pd
import sys
import shutil
from pathlib import Path
from minio import Minio
from sklearn.model_selection import train_test_split

MINIO_ENDPOINT = "localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "nyctaxiproject"
OBJECT_PREFIX = "nyc_clean_for_ML"

def download_from_minio(dest_folder: str):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False  # Important : False car on est en HTTP local, pas HTTPS
    )
    dest_path = Path(dest_folder)
    if dest_path.exists():
        shutil.rmtree(dest_path)
    dest_path.mkdir(parents=True, exist_ok=True)

    print(f"Connecting on MinIO ({MINIO_ENDPOINT}) on '{BUCKET_NAME}' bucket...")
    objects = client.list_objects(BUCKET_NAME, prefix=OBJECT_PREFIX, recursive=True)

    found_files = False
    for obj in objects:
        # On ignore les dossiers virtuels et les fichiers de succès Spark (_SUCCESS)
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

def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
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

def load_data(filepath: str, test_rate=0.2) -> pd.DataFrame:
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

if __name__ == "__main__":
    features = ['trip_distance', 'PULocationID', 'DOLocationID', 'pickup_hour', 'day_of_week', 'total_amount']
    target = features.pop()
    print(target)
    print(features)