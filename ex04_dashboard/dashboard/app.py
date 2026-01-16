import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine
from datetime import datetime

# =============================================================================
# Configuration
# =============================================================================
st.set_page_config(
    page_title="Dashboard Taxi NYC",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #FFD700;
        text-align: center;
        padding: 1rem 0;
    }
    .metric-card {
        background-color: #1E1E1E;
        border-radius: 10px;
        padding: 1rem;
        border-left: 4px solid #FFD700;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #262730;
        border-radius: 8px 8px 0 0;
        padding: 10px 20px;
    }
</style>
""", unsafe_allow_html=True)

# Database connection
@st.cache_resource
def get_db_connection():
    engine = create_engine(
        "postgresql://user_dw:password_dw@postgres-dw:5432/nyc_data_warehouse"
    )
    return engine

# =============================================================================
# Fonctions de chargement des données
# =============================================================================

@st.cache_data(ttl=300)
def load_kpis():
    """Charger les KPIs principaux"""
    engine = get_db_connection()
    query = """
    SELECT
        COUNT(*) as total_trips,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_fare,
        AVG(trip_distance) as avg_distance,
        AVG(tip_ratio) * 100 as avg_tip_pct,
        AVG(trip_duration_minutes) as avg_duration,
        AVG(avg_speed_mph) as avg_speed,
        SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) as outlier_count,
        SUM(CASE WHEN NOT is_outlier THEN 1 ELSE 0 END) as clean_count
    FROM dw.fact_trip
    """
    return pd.read_sql(query, engine).iloc[0]

@st.cache_data(ttl=300)
def load_daily_trips():
    """Charger les courses journalières"""
    engine = get_db_connection()
    query = """
    SELECT
        d.full_date,
        d.day_name,
        d.is_weekend,
        SUM(v.trips_count) as trips,
        SUM(v.total_total_amount) as revenue,
        AVG(v.avg_fare_amount) as avg_fare
    FROM dw.fact_vendor_daily v
    JOIN dw.dim_date d ON v.date_id = d.date_id
    GROUP BY d.full_date, d.day_name, d.is_weekend
    ORDER BY d.full_date
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_vendor_stats():
    """Charger les statistiques par vendeur"""
    engine = get_db_connection()
    query = """
    SELECT
        v.vendor_name,
        SUM(f.trips_count) as trips,
        SUM(f.total_total_amount) as revenue,
        AVG(f.avg_fare_amount) as avg_fare,
        AVG(f.avg_tip_ratio) * 100 as avg_tip_pct
    FROM dw.fact_vendor_daily f
    JOIN dw.dim_vendor v ON f.vendor_id = v.vendor_id
    GROUP BY v.vendor_name
    ORDER BY revenue DESC
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_top_pickup_zones(limit=15):
    """Charger les zones de prise en charge les plus fréquentes"""
    engine = get_db_connection()
    query = f"""
    SELECT
        l.borough,
        l.zone,
        SUM(f.trips_count) as trips,
        SUM(f.total_total_amount) as revenue,
        AVG(f.avg_fare_amount) as avg_fare
    FROM dw.fact_daily_pickup_zone f
    JOIN dw.dim_location l ON f.pickup_location_id = l.location_id
    GROUP BY l.borough, l.zone
    ORDER BY trips DESC
    LIMIT {limit}
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_top_dropoff_zones(limit=15):
    """Charger les zones de dépose les plus fréquentes"""
    engine = get_db_connection()
    query = f"""
    SELECT
        l.borough,
        l.zone,
        SUM(f.trips_count) as trips,
        SUM(f.total_total_amount) as revenue
    FROM dw.fact_daily_dropoff_zone f
    JOIN dw.dim_location l ON f.dropoff_location_id = l.location_id
    GROUP BY l.borough, l.zone
    ORDER BY trips DESC
    LIMIT {limit}
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_borough_stats():
    """Charger les statistiques par arrondissement"""
    engine = get_db_connection()
    query = """
    SELECT
        l.borough,
        SUM(f.trips_count) as trips,
        SUM(f.total_total_amount) as revenue,
        AVG(f.avg_fare_amount) as avg_fare
    FROM dw.fact_daily_pickup_zone f
    JOIN dw.dim_location l ON f.pickup_location_id = l.location_id
    WHERE l.borough IS NOT NULL AND l.borough != 'Unknown'
    GROUP BY l.borough
    ORDER BY trips DESC
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_payment_stats():
    """Charger les statistiques par type de paiement"""
    engine = get_db_connection()
    query = """
    SELECT
        p.description as payment_type,
        COUNT(*) as trips,
        SUM(total_amount) as revenue,
        AVG(tip_ratio) * 100 as avg_tip_pct
    FROM dw.fact_trip f
    JOIN dw.dim_payment_type p ON f.payment_type_id = p.payment_type_id
    WHERE NOT f.is_outlier
    GROUP BY p.description
    ORDER BY trips DESC
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_hourly_distribution():
    """Charger la distribution horaire des courses"""
    engine = get_db_connection()
    query = """
    SELECT
        EXTRACT(HOUR FROM tpep_pickup_datetime) as hour,
        COUNT(*) as trips,
        AVG(total_amount) as avg_fare,
        AVG(trip_duration_minutes) as avg_duration
    FROM dw.fact_trip
    WHERE is_outlier = FALSE
    GROUP BY EXTRACT(HOUR FROM tpep_pickup_datetime)
    ORDER BY hour
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_weekday_stats():
    """Charger les statistiques par jour de la semaine"""
    engine = get_db_connection()
    query = """
    SELECT
        d.day_name,
        d.day_of_week,
        SUM(v.trips_count) as trips,
        SUM(v.total_total_amount) as revenue,
        AVG(v.avg_tip_ratio) * 100 as avg_tip_pct
    FROM dw.fact_vendor_daily v
    JOIN dw.dim_date d ON v.date_id = d.date_id
    GROUP BY d.day_name, d.day_of_week
    ORDER BY d.day_of_week
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_date_range():
    """Charger la plage de dates des données"""
    engine = get_db_connection()
    query = """
    SELECT MIN(full_date) as min_date, MAX(full_date) as max_date
    FROM dw.dim_date
    """
    return pd.read_sql(query, engine).iloc[0]

@st.cache_data(ttl=300)
def load_ratecode_stats():
    """Charger les statistiques par code tarifaire"""
    engine = get_db_connection()
    query = """
    SELECT
        r.description as ratecode,
        COUNT(*) as trips,
        AVG(total_amount) as avg_fare,
        AVG(trip_distance) as avg_distance,
        AVG(trip_duration_minutes) as avg_duration
    FROM dw.fact_trip f
    JOIN dw.dim_ratecode r ON f.ratecode_id = r.ratecode_id
    WHERE NOT f.is_outlier
    GROUP BY r.description
    ORDER BY trips DESC
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_distance_distribution():
    """Charger la distribution des distances"""
    engine = get_db_connection()
    query = """
    SELECT
        CASE
            WHEN trip_distance < 1 THEN '0-1 km'
            WHEN trip_distance < 2 THEN '1-3 km'
            WHEN trip_distance < 5 THEN '3-8 km'
            WHEN trip_distance < 10 THEN '8-16 km'
            WHEN trip_distance < 20 THEN '16-32 km'
            ELSE '32+ km'
        END as distance_range,
        CASE
            WHEN trip_distance < 1 THEN 1
            WHEN trip_distance < 2 THEN 2
            WHEN trip_distance < 5 THEN 3
            WHEN trip_distance < 10 THEN 4
            WHEN trip_distance < 20 THEN 5
            ELSE 6
        END as sort_order,
        COUNT(*) as trips,
        AVG(total_amount) as avg_fare
    FROM dw.fact_trip
    WHERE NOT is_outlier
    GROUP BY distance_range, sort_order
    ORDER BY sort_order
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_fare_distribution():
    """Charger la distribution des tarifs"""
    engine = get_db_connection()
    query = """
    SELECT
        CASE
            WHEN total_amount < 10 THEN '0-10$'
            WHEN total_amount < 20 THEN '10-20$'
            WHEN total_amount < 30 THEN '20-30$'
            WHEN total_amount < 50 THEN '30-50$'
            WHEN total_amount < 100 THEN '50-100$'
            ELSE '100$+'
        END as fare_range,
        CASE
            WHEN total_amount < 10 THEN 1
            WHEN total_amount < 20 THEN 2
            WHEN total_amount < 30 THEN 3
            WHEN total_amount < 50 THEN 4
            WHEN total_amount < 100 THEN 5
            ELSE 6
        END as sort_order,
        COUNT(*) as trips
    FROM dw.fact_trip
    WHERE NOT is_outlier
    GROUP BY fare_range, sort_order
    ORDER BY sort_order
    """
    return pd.read_sql(query, engine)

# =============================================================================
# Fonctions d'analyse des outliers
# =============================================================================

@st.cache_data(ttl=300)
def load_outlier_summary():
    """Charger le résumé des outliers par raison"""
    engine = get_db_connection()
    query = """
    SELECT
        outlier_reason,
        COUNT(*) as count,
        AVG(total_amount) as avg_fare,
        AVG(trip_distance) as avg_distance,
        AVG(trip_duration_minutes) as avg_duration,
        AVG(avg_speed_mph) as avg_speed
    FROM dw.fact_trip
    WHERE is_outlier = TRUE
    GROUP BY outlier_reason
    ORDER BY count DESC
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_outlier_samples(reason=None, limit=100):
    """Charger les échantillons d'outliers avec détail complet des prix"""
    engine = get_db_connection()
    where_clause = "WHERE is_outlier = TRUE"
    if reason and reason != "Tous":
        where_clause += f" AND outlier_reason = '{reason}'"

    query = f"""
    SELECT
        trip_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        trip_distance,
        trip_duration_minutes,
        avg_speed_mph,
        passenger_count,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        cbd_congestion_fee,
        total_amount,
        outlier_reason,
        pl.zone as pickup_zone,
        dl.zone as dropoff_zone,
        pt.description as payment_type
    FROM dw.fact_trip f
    LEFT JOIN dw.dim_location pl ON f.pickup_location_id = pl.location_id
    LEFT JOIN dw.dim_location dl ON f.dropoff_location_id = dl.location_id
    LEFT JOIN dw.dim_payment_type pt ON f.payment_type_id = pt.payment_type_id
    {where_clause}
    ORDER BY ABS(total_amount) DESC
    LIMIT {limit}
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_outlier_vs_normal_comparison():
    """Comparer outliers vs courses normales"""
    engine = get_db_connection()
    query = """
    SELECT
        CASE WHEN is_outlier THEN 'Outlier' ELSE 'Normal' END as category,
        COUNT(*) as trips,
        AVG(total_amount) as avg_fare,
        AVG(trip_distance) as avg_distance,
        AVG(trip_duration_minutes) as avg_duration,
        AVG(tip_ratio) * 100 as avg_tip_pct,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_amount) as median_fare,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trip_distance) as median_distance
    FROM dw.fact_trip
    GROUP BY is_outlier
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_outlier_by_hour():
    """Charger la distribution des outliers par heure"""
    engine = get_db_connection()
    query = """
    SELECT
        EXTRACT(HOUR FROM tpep_pickup_datetime) as hour,
        SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) as outlier_count,
        SUM(CASE WHEN NOT is_outlier THEN 1 ELSE 0 END) as normal_count,
        ROUND(100.0 * SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) / COUNT(*), 2) as outlier_pct
    FROM dw.fact_trip
    GROUP BY EXTRACT(HOUR FROM tpep_pickup_datetime)
    ORDER BY hour
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_outlier_by_vendor():
    """Charger la distribution des outliers par vendeur"""
    engine = get_db_connection()
    query = """
    SELECT
        v.vendor_name,
        SUM(CASE WHEN f.is_outlier THEN 1 ELSE 0 END) as outlier_count,
        SUM(CASE WHEN NOT f.is_outlier THEN 1 ELSE 0 END) as normal_count,
        ROUND(100.0 * SUM(CASE WHEN f.is_outlier THEN 1 ELSE 0 END) / COUNT(*), 2) as outlier_pct
    FROM dw.fact_trip f
    JOIN dw.dim_vendor v ON f.vendor_id = v.vendor_id
    GROUP BY v.vendor_name
    ORDER BY outlier_pct DESC
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_extreme_values():
    """Charger les valeurs extrêmes"""
    engine = get_db_connection()
    query = """
    SELECT
        'Distance max' as metric,
        MAX(trip_distance) as value,
        'km' as unit
    FROM dw.fact_trip WHERE is_outlier
    UNION ALL
    SELECT
        'Durée max',
        MAX(trip_duration_minutes),
        'min'
    FROM dw.fact_trip WHERE is_outlier
    UNION ALL
    SELECT
        'Tarif max',
        MAX(total_amount),
        '$'
    FROM dw.fact_trip WHERE is_outlier
    UNION ALL
    SELECT
        'Vitesse max',
        MAX(avg_speed_mph),
        'km/h'
    FROM dw.fact_trip WHERE is_outlier
    UNION ALL
    SELECT
        'Tarif min (négatif)',
        MIN(total_amount),
        '$'
    FROM dw.fact_trip WHERE is_outlier AND total_amount < 0
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_top_routes():
    """Charger les trajets les plus fréquents"""
    engine = get_db_connection()
    query = """
    SELECT
        pl.zone as pickup_zone,
        dl.zone as dropoff_zone,
        COUNT(*) as trips,
        AVG(total_amount) as avg_fare,
        AVG(trip_distance) as avg_distance
    FROM dw.fact_trip f
    JOIN dw.dim_location pl ON f.pickup_location_id = pl.location_id
    JOIN dw.dim_location dl ON f.dropoff_location_id = dl.location_id
    WHERE NOT f.is_outlier
    GROUP BY pl.zone, dl.zone
    ORDER BY trips DESC
    LIMIT 20
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_passenger_stats():
    """Charger les statistiques par nombre de passagers"""
    engine = get_db_connection()
    query = """
    SELECT
        passenger_count,
        COUNT(*) as trips,
        AVG(total_amount) as avg_fare,
        AVG(tip_ratio) * 100 as avg_tip_pct
    FROM dw.fact_trip
    WHERE NOT is_outlier AND passenger_count > 0 AND passenger_count <= 6
    GROUP BY passenger_count
    ORDER BY passenger_count
    """
    return pd.read_sql(query, engine)

# =============================================================================
# Traduction des jours de la semaine
# =============================================================================
DAY_TRANSLATION = {
    'Monday': 'Lundi',
    'Tuesday': 'Mardi',
    'Wednesday': 'Mercredi',
    'Thursday': 'Jeudi',
    'Friday': 'Vendredi',
    'Saturday': 'Samedi',
    'Sunday': 'Dimanche'
}

def translate_day(day_name):
    """Traduire le nom du jour en français"""
    return DAY_TRANSLATION.get(day_name.strip(), day_name)

# =============================================================================
# Mise en page du Dashboard
# =============================================================================

def render_overview_tab():
    """Afficher l'onglet Vue d'ensemble"""
    st.header("Indicateurs Clés de Performance")

    kpis = load_kpis()

    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.metric("Total Courses", f"{kpis['total_trips']:,.0f}")
    with col2:
        st.metric("Revenu Total", f"{kpis['total_revenue']:,.0f}$")
    with col3:
        st.metric("Tarif Moyen", f"{kpis['avg_fare']:.2f}$")
    with col4:
        st.metric("Distance Moy.", f"{kpis['avg_distance']*1.6:.1f} km")
    with col5:
        st.metric("Durée Moy.", f"{kpis['avg_duration']:.1f} min")
    with col6:
        st.metric("Pourboire Moy.", f"{kpis['avg_tip_pct']:.1f}%")

    st.divider()

    # Tendances journalières
    st.subheader("Tendances Journalières")
    daily_data = load_daily_trips()

    col1, col2 = st.columns(2)

    with col1:
        fig_trips = px.line(
            daily_data, x='full_date', y='trips',
            title='Nombre de Courses par Jour',
            labels={'full_date': 'Date', 'trips': 'Courses'}
        )
        fig_trips.update_traces(line_color='#FFD700')
        fig_trips.update_layout(height=350)
        st.plotly_chart(fig_trips, use_container_width=True)

    with col2:
        fig_revenue = px.line(
            daily_data, x='full_date', y='revenue',
            title='Revenu Journalier',
            labels={'full_date': 'Date', 'revenue': 'Revenu ($)'}
        )
        fig_revenue.update_traces(line_color='#32CD32')
        fig_revenue.update_layout(height=350)
        st.plotly_chart(fig_revenue, use_container_width=True)

    # Résumé qualité des données
    st.divider()
    st.subheader("Qualité des Données")

    col1, col2, col3 = st.columns(3)
    total = kpis['total_trips']
    clean = kpis['clean_count']
    outliers = kpis['outlier_count']

    with col1:
        st.metric("Données Valides", f"{clean:,.0f}", f"{clean/total*100:.1f}%")
    with col2:
        st.metric("Outliers Détectés", f"{outliers:,.0f}", f"{outliers/total*100:.1f}%")
    with col3:
        st.metric("Score Qualité", f"{clean/total*100:.1f}%")


def render_geographic_tab():
    """Afficher l'onglet Analyse Géographique"""
    st.header("Analyse Géographique")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Zones de Prise en Charge")
        pickup_data = load_top_pickup_zones()
        fig_pickup = px.bar(
            pickup_data.head(10), x='trips', y='zone',
            orientation='h', title='Top 10 Zones de Départ',
            color='trips', color_continuous_scale='YlOrRd'
        )
        fig_pickup.update_layout(height=400, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_pickup, use_container_width=True)

    with col2:
        st.subheader("Zones de Dépose")
        dropoff_data = load_top_dropoff_zones()
        fig_dropoff = px.bar(
            dropoff_data.head(10), x='trips', y='zone',
            orientation='h', title='Top 10 Zones d\'Arrivée',
            color='trips', color_continuous_scale='Greens'
        )
        fig_dropoff.update_layout(height=400, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_dropoff, use_container_width=True)

    # Analyse par arrondissement
    st.divider()
    st.subheader("Analyse par Arrondissement")

    borough_data = load_borough_stats()

    col1, col2 = st.columns(2)

    with col1:
        fig_borough_trips = px.pie(
            borough_data, values='trips', names='borough',
            title='Courses par Arrondissement',
            color_discrete_sequence=px.colors.sequential.YlOrRd
        )
        st.plotly_chart(fig_borough_trips, use_container_width=True)

    with col2:
        fig_borough_rev = px.bar(
            borough_data, x='borough', y='revenue',
            title='Revenu par Arrondissement',
            color='avg_fare', color_continuous_scale='Greens',
            labels={'borough': 'Arrondissement', 'revenue': 'Revenu ($)', 'avg_fare': 'Tarif moy.'}
        )
        st.plotly_chart(fig_borough_rev, use_container_width=True)

    # Trajets les plus fréquents
    st.divider()
    st.subheader("Trajets les Plus Fréquents")
    routes = load_top_routes()
    routes['route'] = routes['pickup_zone'] + ' → ' + routes['dropoff_zone']

    fig_routes = px.bar(
        routes.head(15), x='trips', y='route',
        orientation='h', title='Top 15 Trajets par Nombre de Courses',
        color='avg_fare', color_continuous_scale='Viridis',
        labels={'trips': 'Courses', 'route': 'Trajet', 'avg_fare': 'Tarif moy.'}
    )
    fig_routes.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig_routes, use_container_width=True)


def render_time_tab():
    """Afficher l'onglet Analyse Temporelle"""
    st.header("Analyse Temporelle")

    col1, col2 = st.columns(2)

    with col1:
        hourly_data = load_hourly_distribution()
        fig_hourly = px.bar(
            hourly_data, x='hour', y='trips',
            title='Courses par Heure de la Journée',
            color='avg_fare', color_continuous_scale='YlOrRd',
            labels={'hour': 'Heure', 'trips': 'Courses', 'avg_fare': 'Tarif moy.'}
        )
        fig_hourly.update_layout(height=400)
        st.plotly_chart(fig_hourly, use_container_width=True)

    with col2:
        weekday_data = load_weekday_stats()
        weekday_data['jour'] = weekday_data['day_name'].apply(translate_day)
        fig_weekday = px.bar(
            weekday_data, x='jour', y='trips',
            title='Courses par Jour de la Semaine',
            color='revenue', color_continuous_scale='Greens',
            labels={'jour': 'Jour', 'trips': 'Courses', 'revenue': 'Revenu'}
        )
        fig_weekday.update_layout(height=400)
        st.plotly_chart(fig_weekday, use_container_width=True)

    # Détails horaires
    st.divider()
    st.subheader("Détails Horaires")

    col1, col2 = st.columns(2)

    with col1:
        fig_hourly_fare = px.line(
            hourly_data, x='hour', y='avg_fare',
            title='Tarif Moyen par Heure',
            markers=True,
            labels={'hour': 'Heure', 'avg_fare': 'Tarif moyen ($)'}
        )
        fig_hourly_fare.update_traces(line_color='#FFD700')
        st.plotly_chart(fig_hourly_fare, use_container_width=True)

    with col2:
        fig_hourly_duration = px.line(
            hourly_data, x='hour', y='avg_duration',
            title='Durée Moyenne par Heure',
            markers=True,
            labels={'hour': 'Heure', 'avg_duration': 'Durée moyenne (min)'}
        )
        fig_hourly_duration.update_traces(line_color='#32CD32')
        st.plotly_chart(fig_hourly_duration, use_container_width=True)


def render_vendors_tab():
    """Afficher l'onglet Vendeurs & Paiements"""
    st.header("Analyse Vendeurs & Paiements")

    col1, col2 = st.columns(2)

    with col1:
        vendor_data = load_vendor_stats()
        fig_vendor = px.pie(
            vendor_data, values='trips', names='vendor_name',
            title='Courses par Vendeur',
            color_discrete_sequence=px.colors.sequential.YlOrRd
        )
        fig_vendor.update_layout(height=400)
        st.plotly_chart(fig_vendor, use_container_width=True)

    with col2:
        payment_data = load_payment_stats()
        # Traduire les types de paiement
        payment_translation = {
            'Credit card': 'Carte de crédit',
            'Cash': 'Espèces',
            'No charge': 'Gratuit',
            'Dispute': 'Litige',
            'Unknown': 'Inconnu',
            'Voided trip': 'Course annulée',
            'Flex Fare trip': 'Tarif flexible'
        }
        payment_data['type_paiement'] = payment_data['payment_type'].map(
            lambda x: payment_translation.get(x, x)
        )
        fig_payment = px.pie(
            payment_data, values='trips', names='type_paiement',
            title='Courses par Type de Paiement',
            color_discrete_sequence=px.colors.sequential.Greens
        )
        fig_payment.update_layout(height=400)
        st.plotly_chart(fig_payment, use_container_width=True)

    # Tableau de performance des vendeurs
    st.divider()
    st.subheader("Performance des Vendeurs")
    vendor_display = vendor_data.copy()
    vendor_display['revenue'] = vendor_display['revenue'].apply(lambda x: f"{x:,.0f}$")
    vendor_display['avg_fare'] = vendor_display['avg_fare'].apply(lambda x: f"{x:.2f}$")
    vendor_display['avg_tip_pct'] = vendor_display['avg_tip_pct'].apply(lambda x: f"{x:.1f}%")
    vendor_display.columns = ['Vendeur', 'Courses', 'Revenu', 'Tarif Moy.', 'Pourboire Moy.']
    st.dataframe(vendor_display, use_container_width=True, hide_index=True)

    # Analyse des codes tarifaires
    st.divider()
    st.subheader("Analyse des Codes Tarifaires")
    ratecode_data = load_ratecode_stats()

    # Traduire les codes tarifaires
    ratecode_translation = {
        'Standard rate': 'Tarif standard',
        'JFK': 'JFK (Aéroport)',
        'Newark': 'Newark (Aéroport)',
        'Nassau or Westchester': 'Nassau/Westchester',
        'Negotiated fare': 'Tarif négocié',
        'Group ride': 'Course groupée',
        'Unknown': 'Inconnu'
    }
    ratecode_data['code_tarif'] = ratecode_data['ratecode'].map(
        lambda x: ratecode_translation.get(x, x)
    )

    fig_ratecode = px.bar(
        ratecode_data, x='code_tarif', y='trips',
        title='Courses par Code Tarifaire',
        color='avg_fare', color_continuous_scale='Viridis',
        labels={'code_tarif': 'Code Tarifaire', 'trips': 'Courses', 'avg_fare': 'Tarif moy.'}
    )
    st.plotly_chart(fig_ratecode, use_container_width=True)

    # Tableau des codes tarifaires
    ratecode_display = ratecode_data.copy()
    ratecode_display['avg_fare'] = ratecode_display['avg_fare'].apply(lambda x: f"{x:.2f}$")
    ratecode_display['avg_distance'] = ratecode_display['avg_distance'].apply(lambda x: f"{x*1.6:.1f} km")
    ratecode_display['avg_duration'] = ratecode_display['avg_duration'].apply(lambda x: f"{x:.1f} min")
    ratecode_display = ratecode_display[['code_tarif', 'trips', 'avg_fare', 'avg_distance', 'avg_duration']]
    ratecode_display.columns = ['Code Tarifaire', 'Courses', 'Tarif Moy.', 'Distance Moy.', 'Durée Moy.']
    st.dataframe(ratecode_display, use_container_width=True, hide_index=True)


def render_distributions_tab():
    """Afficher l'onglet Distributions"""
    st.header("Distribution des Courses")

    col1, col2 = st.columns(2)

    with col1:
        distance_data = load_distance_distribution()
        fig_distance = px.bar(
            distance_data, x='distance_range', y='trips',
            title='Courses par Tranche de Distance',
            color='avg_fare', color_continuous_scale='YlOrRd',
            labels={'distance_range': 'Distance', 'trips': 'Courses', 'avg_fare': 'Tarif moy.'}
        )
        st.plotly_chart(fig_distance, use_container_width=True)

    with col2:
        fare_data = load_fare_distribution()
        fig_fare = px.bar(
            fare_data, x='fare_range', y='trips',
            title='Courses par Tranche de Tarif',
            color='trips', color_continuous_scale='Greens',
            labels={'fare_range': 'Tarif', 'trips': 'Courses'}
        )
        st.plotly_chart(fig_fare, use_container_width=True)

    # Analyse par nombre de passagers
    st.divider()
    st.subheader("Analyse par Nombre de Passagers")

    passenger_data = load_passenger_stats()

    col1, col2 = st.columns(2)

    with col1:
        fig_passenger_trips = px.bar(
            passenger_data, x='passenger_count', y='trips',
            title='Courses par Nombre de Passagers',
            color='avg_fare', color_continuous_scale='YlOrRd',
            labels={'passenger_count': 'Passagers', 'trips': 'Courses', 'avg_fare': 'Tarif moy.'}
        )
        st.plotly_chart(fig_passenger_trips, use_container_width=True)

    with col2:
        fig_passenger_tip = px.bar(
            passenger_data, x='passenger_count', y='avg_tip_pct',
            title='Pourboire Moyen par Nombre de Passagers',
            color='avg_tip_pct', color_continuous_scale='Greens',
            labels={'passenger_count': 'Passagers', 'avg_tip_pct': 'Pourboire (%)'}
        )
        st.plotly_chart(fig_passenger_tip, use_container_width=True)


def render_outliers_tab():
    """Afficher l'onglet Analyse des Outliers"""
    st.header("Analyse des Outliers")

    st.info("""
    Cette section analyse les courses identifiées comme outliers. Celles-ci peuvent inclure :
    - Courses extrêmement longues ou courtes
    - Tarifs anormalement élevés ou bas
    - Vitesses impossibles
    - Erreurs de saisie de données

    Examinez ces données pour déterminer s'il s'agit de véritables anomalies ou de problèmes de qualité des données.
    """)

    # Comparaison globale
    comparison = load_outlier_vs_normal_comparison()

    st.subheader("Comparaison Outliers vs Courses Normales")

    col1, col2, col3, col4 = st.columns(4)

    outlier_row = comparison[comparison['category'] == 'Outlier'].iloc[0] if len(comparison[comparison['category'] == 'Outlier']) > 0 else None
    normal_row = comparison[comparison['category'] == 'Normal'].iloc[0] if len(comparison[comparison['category'] == 'Normal']) > 0 else None

    if outlier_row is not None and normal_row is not None:
        with col1:
            st.metric(
                "Nombre d'Outliers",
                f"{outlier_row['trips']:,.0f}",
                f"{outlier_row['trips']/(outlier_row['trips']+normal_row['trips'])*100:.1f}% du total"
            )
        with col2:
            st.metric(
                "Tarif Moy. (Outliers)",
                f"{outlier_row['avg_fare']:.2f}$",
                f"{outlier_row['avg_fare'] - normal_row['avg_fare']:+.2f}$ vs normal"
            )
        with col3:
            st.metric(
                "Distance Moy. (Outliers)",
                f"{outlier_row['avg_distance']*1.6:.1f} km",
                f"{(outlier_row['avg_distance'] - normal_row['avg_distance'])*1.6:+.1f} km vs normal"
            )
        with col4:
            st.metric(
                "Durée Moy. (Outliers)",
                f"{outlier_row['avg_duration']:.1f} min",
                f"{outlier_row['avg_duration'] - normal_row['avg_duration']:+.1f} min vs normal"
            )

    st.divider()

    # Répartition des outliers par raison
    st.subheader("Outliers par Raison")

    outlier_summary = load_outlier_summary()

    if not outlier_summary.empty:
        col1, col2 = st.columns(2)

        with col1:
            fig_reasons = px.pie(
                outlier_summary, values='count', names='outlier_reason',
                title='Répartition des Outliers par Raison',
                color_discrete_sequence=px.colors.sequential.Reds
            )
            st.plotly_chart(fig_reasons, use_container_width=True)

        with col2:
            fig_reasons_bar = px.bar(
                outlier_summary, x='outlier_reason', y='count',
                title='Nombre d\'Outliers par Raison',
                color='avg_fare', color_continuous_scale='Reds',
                labels={'outlier_reason': 'Raison', 'count': 'Nombre', 'avg_fare': 'Tarif moy.'}
            )
            fig_reasons_bar.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_reasons_bar, use_container_width=True)

        # Tableau récapitulatif
        st.subheader("Détails par Raison d'Outlier")
        summary_display = outlier_summary.copy()
        summary_display['avg_fare'] = summary_display['avg_fare'].apply(lambda x: f"{x:.2f}$" if pd.notna(x) else "N/A")
        summary_display['avg_distance'] = summary_display['avg_distance'].apply(lambda x: f"{x*1.6:.1f} km" if pd.notna(x) else "N/A")
        summary_display['avg_duration'] = summary_display['avg_duration'].apply(lambda x: f"{x:.1f} min" if pd.notna(x) else "N/A")
        summary_display['avg_speed'] = summary_display['avg_speed'].apply(lambda x: f"{x*1.6:.1f} km/h" if pd.notna(x) else "N/A")
        summary_display.columns = ['Raison', 'Nombre', 'Tarif Moy.', 'Distance Moy.', 'Durée Moy.', 'Vitesse Moy.']
        st.dataframe(summary_display, use_container_width=True, hide_index=True)
    else:
        st.warning("Aucun outlier trouvé dans les données.")

    st.divider()

    # Outliers par heure et vendeur
    st.subheader("Patterns des Outliers")

    col1, col2 = st.columns(2)

    with col1:
        hourly_outliers = load_outlier_by_hour()
        fig_hourly = px.bar(
            hourly_outliers, x='hour', y='outlier_pct',
            title='Pourcentage d\'Outliers par Heure',
            color='outlier_pct', color_continuous_scale='Reds',
            labels={'hour': 'Heure', 'outlier_pct': 'Outliers (%)'}
        )
        fig_hourly.update_layout(yaxis_title="Outliers (%)")
        st.plotly_chart(fig_hourly, use_container_width=True)

    with col2:
        vendor_outliers = load_outlier_by_vendor()
        fig_vendor = px.bar(
            vendor_outliers, x='vendor_name', y='outlier_pct',
            title='Pourcentage d\'Outliers par Vendeur',
            color='outlier_pct', color_continuous_scale='Reds',
            labels={'vendor_name': 'Vendeur', 'outlier_pct': 'Outliers (%)'}
        )
        fig_vendor.update_layout(xaxis_tickangle=-45, yaxis_title="Outliers (%)")
        st.plotly_chart(fig_vendor, use_container_width=True)

    st.divider()

    # Valeurs extrêmes
    st.subheader("Valeurs Extrêmes dans les Outliers")
    extreme = load_extreme_values()

    cols = st.columns(5)
    for i, row in extreme.iterrows():
        with cols[i % 5]:
            if row['unit'] == '$':
                st.metric(row['metric'], f"{row['value']:,.2f}$")
            elif row['unit'] == 'km':
                st.metric(row['metric'], f"{row['value']*1.6:,.1f} km")
            elif row['unit'] == 'min':
                st.metric(row['metric'], f"{row['value']:,.0f} min")
            elif row['unit'] == 'km/h':
                st.metric(row['metric'], f"{row['value']*1.6:,.1f} km/h")
            else:
                st.metric(row['metric'], f"{row['value']:,.1f} {row['unit']}")

    st.divider()

    # Échantillons de courses outliers avec détail complet des prix
    st.subheader("Échantillons de Courses Outliers - Détail Complet")

    # Filtre par raison
    reasons = ["Tous"] + (outlier_summary['outlier_reason'].tolist() if not outlier_summary.empty else [])
    selected_reason = st.selectbox("Filtrer par raison d'outlier :", reasons)

    sample_limit = st.slider("Nombre d'échantillons à afficher :", 10, 200, 50)

    samples = load_outlier_samples(
        reason=selected_reason if selected_reason != "Tous" else None,
        limit=sample_limit
    )

    if not samples.empty:
        # Informations sur la composition du prix
        st.markdown("""
        **Composition du prix total :**
        - **Tarif de base** : Prix de la course (distance + temps)
        - **Extra** : Suppléments (nuit, heure de pointe)
        - **MTA Tax** : Taxe MTA (0.50$)
        - **Pourboire** : Pourboire du client
        - **Péages** : Frais de péage
        - **Surcharge amélioration** : Taxe d'amélioration (0.30$)
        - **Surcharge congestion** : Taxe congestion zone Manhattan
        - **Frais aéroport** : Supplément aéroport
        - **CBD Congestion Fee** : Nouvelle taxe congestion CBD
        """)

        # Afficher les détails complets
        st.markdown("---")

        # Format pour affichage
        samples_display = samples.copy()

        # Colonnes d'information générale
        samples_display['Heure Départ'] = samples_display['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d %H:%M')
        samples_display['Heure Arrivée'] = samples_display['tpep_dropoff_datetime'].dt.strftime('%Y-%m-%d %H:%M')
        samples_display['Distance (km)'] = samples_display['trip_distance'].apply(lambda x: f"{x*1.6:.2f}" if pd.notna(x) else "N/A")
        samples_display['Durée (min)'] = samples_display['trip_duration_minutes'].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
        samples_display['Vitesse (km/h)'] = samples_display['avg_speed_mph'].apply(lambda x: f"{x*1.6:.1f}" if pd.notna(x) else "N/A")
        samples_display['Passagers'] = samples_display['passenger_count']

        # Colonnes de prix - formatées en dollars
        samples_display['Tarif Base'] = samples_display['fare_amount'].apply(lambda x: f"{x:.2f}$" if pd.notna(x) else "0.00$")
        samples_display['Extra'] = samples_display['extra'].apply(lambda x: f"{x:.2f}$" if pd.notna(x) else "0.00$")
        samples_display['MTA Tax'] = samples_display['mta_tax'].apply(lambda x: f"{x:.2f}$" if pd.notna(x) else "0.00$")
        samples_display['Pourboire'] = samples_display['tip_amount'].apply(lambda x: f"{x:.2f}$" if pd.notna(x) else "0.00$")
        samples_display['Péages'] = samples_display['tolls_amount'].apply(lambda x: f"{x:.2f}$" if pd.notna(x) else "0.00$")
        samples_display['Surcharge Amél.'] = samples_display['improvement_surcharge'].apply(lambda x: f"{x:.2f}$" if pd.notna(x) else "0.00$")
        samples_display['Surcharge Cong.'] = samples_display['congestion_surcharge'].apply(lambda x: f"{x:.2f}$" if pd.notna(x) else "0.00$")
        samples_display['Frais Aéroport'] = samples_display['airport_fee'].apply(lambda x: f"{x:.2f}$" if pd.notna(x) else "0.00$")
        samples_display['CBD Fee'] = samples_display['cbd_congestion_fee'].apply(lambda x: f"{x:.2f}$" if pd.notna(x) else "0.00$")
        samples_display['TOTAL'] = samples_display['total_amount'].apply(lambda x: f"{x:.2f}$" if pd.notna(x) else "N/A")

        # Autres colonnes
        samples_display['Raison Outlier'] = samples_display['outlier_reason']
        samples_display['Zone Départ'] = samples_display['pickup_zone']
        samples_display['Zone Arrivée'] = samples_display['dropoff_zone']
        samples_display['Paiement'] = samples_display['payment_type']

        # Sélectionner les colonnes à afficher
        display_cols = [
            'trip_id', 'Heure Départ', 'Heure Arrivée', 'Zone Départ', 'Zone Arrivée',
            'Distance (km)', 'Durée (min)', 'Vitesse (km/h)', 'Passagers', 'Paiement',
            'Tarif Base', 'Extra', 'MTA Tax', 'Pourboire', 'Péages',
            'Surcharge Amél.', 'Surcharge Cong.', 'Frais Aéroport', 'CBD Fee', 'TOTAL',
            'Raison Outlier'
        ]

        final_display = samples_display[display_cols].copy()
        final_display.columns = [
            'ID', 'Départ', 'Arrivée', 'Zone Départ', 'Zone Arrivée',
            'Distance', 'Durée', 'Vitesse', 'Pass.', 'Paiement',
            'Base', 'Extra', 'MTA', 'Pourboire', 'Péages',
            'Amél.', 'Cong.', 'Aéro.', 'CBD', 'TOTAL',
            'Raison'
        ]

        st.dataframe(final_display, use_container_width=True, hide_index=True)

        # Statistiques rapides sur les prix
        st.subheader("Statistiques des Composantes de Prix (Outliers)")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            avg_base = samples['fare_amount'].mean()
            st.metric("Tarif Base Moyen", f"{avg_base:.2f}$")
        with col2:
            avg_tip = samples['tip_amount'].mean()
            st.metric("Pourboire Moyen", f"{avg_tip:.2f}$")
        with col3:
            avg_tolls = samples['tolls_amount'].mean()
            st.metric("Péages Moyens", f"{avg_tolls:.2f}$")
        with col4:
            avg_total = samples['total_amount'].mean()
            st.metric("Total Moyen", f"{avg_total:.2f}$")

        # Bouton de téléchargement
        csv = samples.to_csv(index=False)
        st.download_button(
            label="Télécharger les Outliers (CSV)",
            data=csv,
            file_name="outliers_details.csv",
            mime="text/csv"
        )
    else:
        st.info("Aucun échantillon d'outlier trouvé pour les critères sélectionnés.")


def main():
    # En-tête
    st.markdown('<p class="main-header">🚕 Dashboard Taxi Jaune NYC</p>', unsafe_allow_html=True)

    # Charger la plage de dates
    try:
        date_range = load_date_range()
        st.caption(f"Période des données : {date_range['min_date']} au {date_range['max_date']}")
    except Exception as e:
        st.error(f"Erreur de connexion à la base de données : {e}")
        st.info("Assurez-vous que PostgreSQL est en cours d'exécution et que l'Ex03 a été exécuté.")
        return

    # Barre latérale
    st.sidebar.header("Contrôles du Dashboard")
    st.sidebar.info("Analyse du Data Warehouse des Taxis Jaunes de NYC")

    if st.sidebar.button("🔄 Actualiser les Données"):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.divider()
    st.sidebar.markdown("### Statistiques Rapides")
    kpis = load_kpis()
    st.sidebar.metric("Total Courses", f"{kpis['total_trips']:,.0f}")
    st.sidebar.metric("Taux d'Outliers", f"{kpis['outlier_count']/kpis['total_trips']*100:.1f}%")

    # Onglets principaux
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Vue d'ensemble",
        "🗺️ Géographie",
        "⏰ Temporel",
        "🏢 Vendeurs & Paiements",
        "📈 Distributions",
        "⚠️ Analyse Outliers"
    ])

    with tab1:
        render_overview_tab()

    with tab2:
        render_geographic_tab()

    with tab3:
        render_time_tab()

    with tab4:
        render_vendors_tab()

    with tab5:
        render_distributions_tab()

    with tab6:
        render_outliers_tab()

    # Pied de page
    st.divider()
    st.caption("Dashboard Data Warehouse Taxi Jaune NYC | Développé avec Streamlit")

if __name__ == "__main__":
    main()
