from data_manager import load_data, prepare_data
from model_manager import train_model, test_model, save_model

if __name__ == "__main__":
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

    model = train_model(X_train, y_train)
    rmse = test_model(model, X_test, y_test)
    save_model(model, rmse)