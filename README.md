# BigYellowData - NYC Yellow Taxi Data Pipeline

A complete Big Data pipeline for NYC Yellow Taxi trip analysis, featuring data ingestion, ETL processing, Data Warehouse, ML prediction service, and workflow orchestration.

## Architecture

```
BigYellowData/
├── ex01_data_retrieval/        # Download NYC taxi data from TLC
├── ex02_data_ingestion/        # Clean & transform with Spark, store in MinIO
├── ex03_sql_table_creation/    # PostgreSQL Data Warehouse (Star Schema)
├── ex04_dashboard/             # Streamlit Analytics Dashboard
├── ex05_ml_prediction_service/ # ML Fare Prediction API
├── ex06_airflow/               # Airflow DAGs for orchestration
├── docker/                     # Spark Docker configuration
├── docker-compose.yml          # All services orchestration
├── setup_and_run.sh            # Main execution script
└── run_spark_docker.sh         # Spark job runner
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| Data Processing | Apache Spark 3.5 (Scala) |
| Data Lake | MinIO (S3-compatible) |
| Data Warehouse | PostgreSQL 15 |
| Dashboard | Streamlit + Plotly |
| ML Service | scikit-learn + Streamlit |
| Orchestration | Apache Airflow 2.x |
| Containerization | Docker Compose |

## Prerequisites

- **Docker** & **Docker Compose** installed
- **Git** with LFS support
- Minimum **8 GB RAM** available
- Free ports: 5432, 5050, 7077, 8081, 8082, 8501, 8502, 9000, 9001

## Quick Start

### 1. Clone & Setup

```bash
git clone <repo-url>
cd BigYellowData
chmod +x setup_and_run.sh run_spark_docker.sh
```

### 2. Configure Environment

The `.env` file contains MinIO credentials (already configured):

```bash
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123
MINIO_ENDPOINT=http://minio:9000
```

### 3. Run the Complete Pipeline

```bash
./setup_and_run.sh all
```

This will:
1. Start infrastructure (Spark, MinIO, PostgreSQL, pgAdmin)
2. Execute Ex01: Download NYC taxi data
3. Execute Ex02: Clean & ingest to MinIO
4. Execute Ex03: Create Data Warehouse
5. Launch Dashboard (Ex04)

## Running Individual Exercises

```bash
# Ex01: Download taxi data from NYC TLC
./setup_and_run.sh ex01

# Ex02: Clean and ingest to MinIO Data Lake
./setup_and_run.sh ex02

# Ex03: Create PostgreSQL Data Warehouse
./setup_and_run.sh ex03

# Ex04: Launch Analytics Dashboard
./setup_and_run.sh ex04

# Ex05: Train ML model
./setup_and_run.sh ex05

# Ex05-app: Launch ML Prediction Service
./setup_and_run.sh ex05-app

# Ex06: Start Airflow Orchestration
./setup_and_run.sh ex06
```

> **Note:** Exercises must run in order (ex01 -> ex02 -> ex03) as each depends on the previous.

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Dashboard | http://localhost:8501 | - |
| ML Prediction | http://localhost:8502 | - |
| Airflow | http://localhost:8082 | admin / admin |
| Spark Master | http://localhost:8081 | - |
| MinIO Console | http://localhost:9001 | minio / minio123 |
| pgAdmin | http://localhost:5050 | admin@admin.com / admin |

### pgAdmin PostgreSQL Connection

- **Host:** `postgres-dw`
- **Port:** `5432`
- **Database:** `nyc_data_warehouse`
- **User:** `user_dw`
- **Password:** `password_dw`

## Data Warehouse Schema

### Dimensions
- `dim_date` - Calendar dimension
- `dim_location` - NYC taxi zones (265 zones)
- `dim_vendor` - Taxi companies
- `dim_ratecode` - Fare rate codes
- `dim_payment_type` - Payment methods

### Fact Tables
- `fact_trip` - Individual trip details with outlier flags
- `fact_vendor_daily` - Daily vendor aggregations
- `fact_daily_pickup_zone` - Daily pickup zone stats
- `fact_daily_dropoff_zone` - Daily dropoff zone stats

## Airflow DAGs

### nyc_taxi_full_pipeline (Manual)
Complete end-to-end pipeline:
- Ex01: Data Retrieval
- Ex02: Data Ingestion
- Ex03: DWH Loading
- Ex05: ML Training

### nyc_taxi_monthly_refresh (Scheduled)
Automatic monthly data refresh:
- Downloads new data from NYC TLC website
- Processes through the full pipeline
- Runs on 1st of each month at 2:00 AM

## Common Commands

```bash
# View logs
docker compose logs -f spark-master
docker compose logs -f dashboard
docker compose logs -f airflow-scheduler

# Stop all services
docker compose down

# Full reset (removes all data)
docker compose down -v
rm -rf minio-data postgres-data

# Rebuild images
docker compose build --no-cache

# Check service status
docker compose ps
```

## Troubleshooting

### Port already in use
```bash
sudo lsof -i :5432
docker stop $(docker ps -aq)
```

### Spark memory errors
Increase Docker memory allocation (Settings > Resources > Memory > 8GB+)

### Dashboard shows "Database connection error"
Ensure Ex03 completed successfully:
```bash
./setup_and_run.sh ex03
```

### Airflow DAG not showing
Check scheduler logs:
```bash
docker logs airflow-scheduler --tail 100
```

## Project Structure Details

### Ex01 - Data Retrieval
Downloads Parquet files from NYC TLC website for specified months.

### Ex02 - Data Ingestion
- Reads raw Parquet files
- Cleans invalid records
- Detects outliers (abnormal trips)
- Calculates derived metrics (speed, duration)
- Stores cleaned data in MinIO

### Ex03 - SQL Table Creation
- Creates star schema in PostgreSQL
- Loads dimension tables
- Ingests fact data from MinIO via Spark
- Generates aggregation tables

### Ex04 - Dashboard
Interactive analytics with:
- KPIs overview
- Geographic analysis (pickup/dropoff zones)
- Temporal patterns (hourly, daily)
- Vendor & payment analysis
- Trip distributions
- Outlier analysis with full price breakdown

### Ex05 - ML Prediction Service
- Trains Random Forest model on trip data
- Predicts fare amount based on trip features
- Streamlit UI for predictions

### Ex06 - Airflow Orchestration
- Orchestrates complete pipeline
- Scheduled monthly data refresh
- Automatic new data detection

---

**BigYellowData Team** | CY Tech 2025-2026
