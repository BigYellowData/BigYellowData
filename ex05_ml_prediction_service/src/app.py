import sys
import os
import pytest
import streamlit as st
import pandas as pd
from datetime import datetime
from inference import load_model, predict_trip_price, MODEL_LOAD_PATH


@st.cache_data
def get_taxi_zones():
    """
    Load and preprocess the taxi zone lookup data.

    This function reads the external CSV file containing location IDs
    and names. It creates a new column 'display_name' combining the
    Zone and Borough for better UI readability. The result is cached
    by Streamlit to improve performance.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the taxi zones with an added
        'display_name' column. Returns an empty DataFrame if the file
        cannot be found or loaded.
    """
    # Path relative to the project root (data/raw/ contains the taxi zones)
    zone_path = "data/raw/taxi_zone_lookup.csv"

    # Fallback: handle case where script is run from inside src/ or ex05 folder
    if not os.path.exists(zone_path):
        # Try from ex05_ml_prediction_service folder
        alt_path = "../data/raw/taxi_zone_lookup.csv"
        if os.path.exists(alt_path):
            zone_path = alt_path

    try:
        df = pd.read_csv(zone_path)
        df['display_name'] = df['Zone'] + " - " + df['Borough']
        return df
    except Exception as e:
        st.error(f"Unable to load taxi zones: {e}")
        return pd.DataFrame()


def main():
    """
    Main function to render the Streamlit application.
    """
    # Configuration of the Streamlit page
    st.set_page_config(
        page_title="NYC Taxi Predictor",
        page_icon="🚖",
        layout="wide"
    )

    # --- Main Application Logic ---

    # 1. Load Data and Model
    df_zones = get_taxi_zones()
    model = None

    try:
        # Load model using the constant imported from inference.py
        model = load_model(MODEL_LOAD_PATH)
    except FileNotFoundError:
        st.warning(
            "Model not found. Please ensure the model is trained and saved."
        )
    except Exception as e:
        st.error(f"Error loading model: {e}")

    # 2. UI Layout
    st.title("NYC Taxi Predictor")

    with st.container():
        col_params, col_map = st.columns([1, 1])

        # Left Column: User Inputs
        with col_params:
            st.subheader("Trip parameters")

            with st.form("trip_form"):
                distance = st.slider(
                    "Trip distance (miles)",
                    0.5,
                    50.0,
                    2.5,
                    step=0.1
                )

                # Default indices handling (JFK=132, Times Sq=237 approx)
                # We use safe defaults (0) if dataframe is empty
                idx_jfk = 131 if not df_zones.empty else 0
                idx_ts = 229 if not df_zones.empty else 0

                pickup_name = st.selectbox(
                    "Pickup location",
                    options=df_zones['display_name'],
                    index=idx_jfk,
                )
                dropoff_name = st.selectbox(
                    "Dropoff location",
                    options=df_zones['display_name'],
                    index=idx_ts
                )

                c1, c2 = st.columns(2)
                d = c1.date_input("Date", datetime.now())
                t = c2.time_input("Time", datetime.now())

                # Form submission button
                submitted = st.form_submit_button(
                    "Predict price",
                    use_container_width=True
                )

    # 3. Prediction Logic (Triggered on form submission)
    if submitted and model is not None:
        if df_zones.empty:
            st.error("Taxi zones data is missing. Cannot predict.")
            st.stop()

        # Retrieve LocationIDs from the selected display names
        pickup_row = df_zones[df_zones['display_name'] == pickup_name]
        pickup_id = pickup_row['LocationID'].values[0]

        dropoff_row = df_zones[df_zones['display_name'] == dropoff_name]
        dropoff_id = dropoff_row['LocationID'].values[0]

        # Construct the datetime string as expected by the model pipeline
        datetime_str = f"{d} {t}"

        # Create the input DataFrame matching the model's expected schema
        input_df = pd.DataFrame({
            'trip_distance': [distance],
            'pickup_location_id': [pickup_id],
            'dropoff_location_id': [dropoff_id],
            'tpep_pickup_datetime': [datetime_str]
        })

        try:
            # Perform inference
            price = predict_trip_price(model, input_df)

            # Display results
            st.divider()
            st.success(f"Estimated price: **${price:.2f}**")
            st.caption(
                f"Trip from **{pickup_name}** to **{dropoff_name}** "
                f"({distance} miles)"
            )

        except Exception as e:
            st.error(f"Error during prediction: {e}")


if __name__ == "__main__":
    print("Running integrity tests before starting the app...")
    exit_code = pytest.main(["-q", "tests/test_inference.py"])

    if exit_code != 0:
        print("CRITICAL: Inference tests failed. Application aborted.")
        sys.exit(1)

    print("Tests passed. Starting Streamlit Application...")
    main()
