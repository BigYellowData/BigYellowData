# Exercise 5: Machine Learning Prediction Service (MLOps)

This module implements a complete Machine Learning pipeline for predicting NYC taxi fares. It is designed following an **industrial MLOps approach**, integrating automatic unit tests, strict data schema validation, and an interactive Streamlit user interface.

## Features

* **Robust Training Pipeline:** Automates data loading, cleaning, and model training with performance evaluation.
* **Secure Inference API:** A prediction engine capable of handling single requests with strict schema validation.
* **Guardrails (Safety Checks):** Unit tests are automatically executed *before* the application or training starts to prevent running unstable code.
* **Interactive Dashboard:** A Streamlit web application to visualize predictions in real-time.
* **Modern Dependency Management:** Project managed with **uv** for fast and reliable environment handling.

## Project Structure

```text
ex05_ml_prediction_service/
├── .venv/                 # Virtual environment managed by uv
├── models/                # Trained model artifacts (.joblib)
├── src/
│   ├── app.py             # Streamlit Application (Entry point)
│   ├── config.py          # Centralized configuration (Paths, Constants)
│   ├── data_manager.py    # Data loading and preparation logic
│   ├── inference.py       # Prediction logic and model loading
│   ├── model_manager.py   # Model evaluation and persistence
│   └── train.py           # Training script (Entry point)
├── tests/
│   ├── test_inference.py  # Unit tests for the inference pipeline
│   └── test_train.py      # Unit tests for the training pipeline
├── pyproject.toml         # Project configuration and dependencies
├── uv.lock                # Lock file for reproducible builds
└── README.md
```

## Setup & Installation
1. ***Instal uv***: If you haven't installed uv yet, run the following command:
```text 
curl -LsSf [https://astral.sh/uv/install.sh](https://astral.sh/uv/install.sh) | sh
```
2. ***Sync Envirionment***: Run the following command at the project root to create the virtual environment and install all dependencies defined in ```pyproject.toml```:
```text
uv sync
```

## Usage
1. ***Model Training***: To train the model, run the training script:
```text
uv run python src/train.py
```
*Note: The script includes a Guardrail. It automatically runs pytest on the training module first. If tests fail, training is aborted to prevent creating corrupted models.*

2. ***Prediction Inference***: To launch the web interface, use the following command:
```text
uv run streamlit run src/app.py
```
*Note: The application also performs a self-test on startup.*