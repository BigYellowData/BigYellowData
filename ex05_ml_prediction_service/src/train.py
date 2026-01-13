import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from data_manager import load_data, prepare_data
import joblib

def main():
    print("Loading and preparing data...")
    data_path = "../data/processed"
    train_raw, test_raw = load_data(data_path)
    X, y = prepare_data(train_raw)
    X_test, y_test = prepare_data(test_raw)

    print("Training model...")
    model = LinearRegression()
    model.fit(X,y)

    print("Evaluating model...")
    predictions = model.predict(X_test)
    rmse = mean_squared_error(y_test, predictions, squared=False)
    print(f"RMSE on test set : {rmse:.2f}")
    if (rmse>10):
        print(f"Model not performing enough to save it")
    else:
        print("Saving model...")
        joblib.dump(model, "models/taxi_price_model.joblib")

if __name__ == "__main__":
    main()