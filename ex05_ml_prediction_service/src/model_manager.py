import pandas as pd
import os
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import joblib

MODEL_NAME = "taxi_price_model"
MODEL_SAVE_PATH = f"models/{MODEL_NAME}.joblib"

def train_model(X_train, y_train, model=None):
    """
    Train the machine learning model.
    If no model is provided, it defaults to a standard Linear Regression.

    Parameters
    ----------
    X_train : pd.DataFrame
        The feature matrix for training (distance, time features, etc.).
    y_train : pd.Series
        The target variable (total_amount) for training.
    model : sklearn.base.BaseEstimator, optional
        An instance of a scikit-learn model. If None, a new LinearRegression
        is instantiated. (Default value = None)

    Returns
    -------
    sklearn.base.BaseEstimator
        The fitted model ready for evaluation or inference.
    """
    print(f"Training model on {len(X_train)} rows...")
    if model is None:
        model = LinearRegression()
    model.fit(X_train, y_train)

    return model

def test_model(model, X_test, y_test):
    """
    Evaluate the model performance on the test set.

    Parameters
    ----------
    model : sklearn.base.BaseEstimator
        The trained model to evaluate.
    X_test : pd.DataFrame
        The feature matrix for testing.
    y_test : pd.Series
        The ground truth target variable.

    Returns
    -------
    float
        The Root Mean Squared Error (RMSE) calculated on the test set.
    """
    print("Evaluating model...")
    predictions = model.predict(X_test)
    rmse = mean_squared_error(y_test, predictions, squared=False)

    return rmse

def save_model(model, rmse):
    """
    Persist the model to disk if performance is acceptable.
    This function acts as a 'Quality Gate'. The model is saved ONLY if
    the RMSE is strictly lower than 10.0.

    Parameters
    ----------
    model : sklearn.base.BaseEstimator
        The trained model object to be serialized.
    rmse : float
        The performance metric used to decide whether to save the model.
    """
    if rmse < 10:
        os.makedirs("models", exist_ok=True)
        joblib.dump(model, MODEL_SAVE_PATH)
        print(f"Model saved to {MODEL_SAVE_PATH}, meets performance criteria (RMSE {rmse:.2f} < 10)")
    else:
        print(f"Model not saved because performance is too low (RMSE {rmse:.2f} >= 10)")