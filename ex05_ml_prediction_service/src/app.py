"""
NYC Taxi Price Prediction Streamlit Application.

This script runs a Streamlit web interface that allows users to input
trip parameters (distance, pickup/dropoff locations, date/time) and
retrieves a prediction from a pre-trained machine learning model.
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from inference import load_model, predict_trip_price

# Configuration of the Streamlit page
st.set_page_config(page_title="NYC Taxi Predictor", page_icon="🚖", layout="wide")

@st.cache_data
def get_taxi_zones():
    """
    Load and preprocess the taxi zone lookup data.

    This function reads the external CSV file containing location IDs and names.
    It creates a new column 'display_name' combining the Zone and Borough for
    better UI readability. The result is cached by Streamlit to improve performance.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the taxi zones with an added 'display_name' column.
        Returns an empty DataFrame if the file cannot be found or loaded.
    """
    zone_path = "../data/external/taxi_zone_lookup.csv"
    try:
        df = pd.read_csv(zone_path)
        df['display_name'] = df['Zone'] + " - " + df['Borough']
        return df
    except Exception as e:
        st.error(f"Impossible de charger les zones de taxi : {e}")
        return pd.DataFrame()


# --- Main Application Logic ---

# 1. Load Data and Model
df_zones = get_taxi_zones()
model = None

try:
    # Attempt to load the trained pipeline
    model = load_model("models/taxi_price_model.joblib")
except FileNotFoundError:
    st.warning("Model not found. Please ensure the model is trained and saved.")

# 2. UI Layout
st.title("NYC Taxi Predictor")

with st.container():
    col_params, col_map = st.columns([1, 1])

    # Left Column: User Inputs
    with col_params:
        st.subheader("Trip parameters")

        with st.form("trip_form"):
            distance = st.slider("Trip distance (miles)", 0.5, 50.0, 2.5, step=0.1)

            # Default indices for JFK (132 usually, adjusted here) and Times Sq
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
            t = c2.time_input("Heure", datetime.now())

            # Form submission button
            submitted = st.form_submit_button("Predict price", use_container_width=True)


# 3. Prediction Logic (Triggered on form submission)
if submitted and model is not None:
    # Retrieve LocationIDs from the selected display names
    pickup_id = df_zones[df_zones['display_name'] == pickup_name]['LocationID'].values[0]
    dropoff_id = df_zones[df_zones['display_name'] == dropoff_name]['LocationID'].values[0]

    # Construct the datetime string as expected by the model pipeline
    datetime_str = f"{d} {t}"

    # Create the input DataFrame matching the model's expected schema
    input_df = pd.DataFrame({
        'trip_distance': [distance],
        'PULocationID': [pickup_id],
        'DOLocationID': [dropoff_id],
        'tpep_pickup_datetime': [datetime_str]
    })

    try:
        # Perform inference
        price = predict_trip_price(model, input_df)

        # Display results
        st.divider()
        st.success(f"Estimated price : **${price:.2f}**")
        st.caption(f"Trip from **{pickup_name}** to **{dropoff_name}** ({distance} miles)")

    except Exception as e:
        st.error(f"Error during prediction : {e}")