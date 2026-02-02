import sys
from pathlib import Path
import pytest
from sklearn.ensemble import RandomForestRegressor
from data_manager import load_data, prepare_data_training
from model_manager import test_model, save_model

# Project root directory (BigYellowData)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_PATH = str(PROJECT_ROOT / "data" / "processed")


def main():
    """
    Execute the training pipeline.

    This function handles the workflow control and error management.
    It relies on specific manager modules for data and model operations.
    """
    print("Loading data...")
    try:
        train_raw, test_raw = load_data(DATA_PATH)
    except Exception as e:
        print(f"Critical Error loading data: {e}")
        sys.exit(1)

    print("Preparing data...")
    try:
        X_train, y_train = prepare_data_training(train_raw)
        X_test, y_test = prepare_data_training(test_raw)
    except Exception as e:
        print(f"Critical Error preparing data: {e}")
        sys.exit(1)

    print(f"Training model on {len(X_train)} rows...")
    # Using Random Forest for better predictions with engineered features
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=15,
        min_samples_split=10,
        n_jobs=-1,
        random_state=42
    )
    model.fit(X_train, y_train)

    # Evaluation
    rmse = test_model(model, X_test, y_test)
    print(f"Validation RMSE: {rmse:.4f}")

    # Persistence
    save_model(model)


if __name__ == "__main__":
    print("Running integrity tests before training...")
    exit_code = pytest.main(["-q", "tests/test_train.py"])

    if exit_code != 0:
        print("CRITICAL: Tests failed. Training aborted.")
        sys.exit(1)

    print("Tests passed. Starting training pipeline...")
    main()
