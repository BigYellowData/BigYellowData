import streamlit as st
import pandas as pd
from datetime import datetime
from inference import load_model, predict_trip_price

st.set_page_config(page_title="NYC Taxi Predictor", page_icon="🚖", layout="wide")

@st.cache_data
def get_taxi_zones():
    zone_path = "../data/external/taxi_zone_lookup.csv"
    try:
        df = pd.read_csv(zone_path)
        # On crée une colonne lisible : "Manhattan - Alphabet City (ID: 4)"
        df['display_name'] = df['Zone'] + " - " + df['Borough']
        return df
    except Exception as e:
        st.error(f"Impossible de charger les zones de taxi : {e}")
        return pd.DataFrame()


df_zones = get_taxi_zones()
model = None
try:
    model = load_model("models/taxi_price_model.joblib")
except FileNotFoundError:
    st.warning("Model not found")

st.title("NYC Taxi Predictor")
with st.container():
    col_params, col_map = st.columns([1, 1])
    with col_params:
        st.subheader("Trip parameters")
        with st.form("trip_form"):
            distance = st.slider("Trip distance (miles)", 0.5, 50.0, 2.5, step=0.1)
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
            # 3. Date et Heure
            c1, c2 = st.columns(2)
            d = c1.date_input("Date", datetime.now())
            t = c2.time_input("Heure", datetime.now())

            submitted = st.form_submit_button("Predict price", use_container_width=True)


if submitted and model is not None:
    pickup_id = df_zones[df_zones['display_name'] == pickup_name]['LocationID'].values[0]
    dropoff_id = df_zones[df_zones['display_name'] == dropoff_name]['LocationID'].values[0]
    datetime_str = f"{d} {t}"
    input_df = pd.DataFrame({
        'trip_distance': [distance],
        'PULocationID': [pickup_id],
        'DOLocationID': [dropoff_id],
        'tpep_pickup_datetime': [datetime_str]
    })

    try:
        price = predict_trip_price(model, input_df)
        st.divider()
        st.success(f"Estimated price : **${price:.2f}**")
        st.caption(f"Trip from **{pickup_name}** to **{dropoff_name}** ({distance} miles)")
    except Exception as e:
        st.error(f"Error during prediction : {e}")