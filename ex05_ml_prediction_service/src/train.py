import pandas as pd
import os
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from data_manager import load_data, prepare_data
import joblib

MODEL_NAME = "taxi_price_model"

def main():
    print("Loading data...")
    try:
        data_path = "../data/processed"
        train_raw, test_raw = load_data(data_path)
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    print("Preparing data...")
    try:
        X_train, y_train = prepare_data(train_raw)
        X_test, y_test = prepare_data(test_raw)
    except Exception as e:
        print(f"Error preparing data: {e}")
        return

    print(f"Training model on {len(X_train)} rows...")
    model = LinearRegression()
    model.fit(X_train,y_train)

    print("Evaluating model...")
    predictions = model.predict(X_test)
    rmse = mean_squared_error(y_test, predictions, squared=False)

    print(f"RMSE on test set : {rmse:.2f}")
    if (rmse<10):
        print("Success! Model meets performance criteria (RMSE < 10)")
        os.makedirs("models", exist_ok=True)
        model_path = f"models/{MODEL_NAME}.joblib"
        joblib.dump(model, model_path)
        print(f"Model saved to {model_path}")
    else:
        print(f"Model not saved because performance is too low (RMSE {rmse:.2f} >= 10)")

if __name__ == "__main__":
    main()