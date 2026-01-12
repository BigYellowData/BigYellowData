import pandas as pd
from pathlib import Path
import sys
from sklearn.model_selection import train_test_split

def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    if df['tpep_pickup_datetime'].dtype == 'object':
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour
    df['day_of_week'] = df['tpep_pickup_datetime'].dt.dayofweek
    features = ['trip_distance', 'PULocationID', 'DOLocationID', 'pickup_hour', 'day_of_week']
    target = 'total_amount'

    return df[features], df[target]

def load_and_split_data(filepath: str, test_rate=0.2) -> pd.DataFrame:
    path_obj = Path(filepath)
    if not path_obj.exists():
        raise FileNotFoundError(f"Le fichier n'a pas été trouvé à l'emplacement : {path_obj.resolve()}")
    try:
        print(f"Loading datas...")
        df = pd.read_parquet(path_obj)
        print(f"Spliting datas...")
        df_train, df_test = train_test_split(df, test_size=test_rate, shuffle=True)
        print(f"Train: {df_train.shape} \nTest: {df_test.shape}\n")

        return df_train, df_test
    except Exception as e:
        print(f"Error while reading parquet files : {e}")
        sys.exit(1)