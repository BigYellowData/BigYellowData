"""
NYC Taxi Data Pipeline DAG

This DAG orchestrates the complete BigYellowData pipeline:
1. Ex01: Data Retrieval - Download NYC taxi data from TLC to MinIO
2. Ex02: Data Ingestion - Clean and transform data with Spark
3. Ex03: SQL Table Creation - Load data into PostgreSQL Data Warehouse
4. Ex05: ML Training - Train the taxi price prediction model

Prerequisites:
- All Spark JARs must be pre-built on the host machine
- Run: ./setup_and_run.sh all (or individual exercises)

Author: BigYellowData Team
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# Container names
SPARK_MASTER = 'spark-master'
POSTGRES_DW = 'postgres-dw'

# Project root mounted in Airflow containers
PROJECT_ROOT = '/project'

# Spark configuration
SPARK_PACKAGES = "org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
SPARK_CONF = "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false"


def check_containers():
    """Verify that required containers are running."""
    import subprocess
    containers = ['spark-master', 'postgres-dw', 'minio']
    for container in containers:
        result = subprocess.run(
            ['docker', 'ps', '--filter', f'name={container}', '--format', '{{.Names}}'],
            capture_output=True, text=True
        )
        if container not in result.stdout:
            raise Exception(f"Container {container} is not running!")
    return True


# =============================================================================
# Main Pipeline DAG
# =============================================================================
with DAG(
    'nyc_taxi_full_pipeline',
    default_args=default_args,
    description='Complete NYC Taxi Data Pipeline - From raw data to ML model',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nyc-taxi', 'etl', 'spark', 'ml', 'pipeline'],
    doc_md="""
    # NYC Taxi Full Pipeline

    This DAG runs the complete BigYellowData ETL pipeline:

    1. **Ex01 - Data Retrieval**: Downloads NYC taxi data to MinIO
    2. **Ex02 - Data Ingestion**: Cleans and transforms data with Spark
    3. **Ex03 - SQL & DWH**: Creates PostgreSQL tables and loads data
    4. **Ex05 - ML Training**: Trains the price prediction model

    ## Prerequisites
    - All Spark JARs must be pre-built and available in spark-master:/opt/spark/
    - Run on host: `./run_spark_docker.sh <exercise>` to build JARs

    ## Usage
    Trigger this DAG manually from the Airflow UI.
    """,
) as dag:

    # =========================================================================
    # Start & Check Prerequisites
    # =========================================================================
    start = EmptyOperator(task_id='start')

    check_infra = PythonOperator(
        task_id='check_infrastructure',
        python_callable=check_containers,
    )

    # =========================================================================
    # Ex01: Data Retrieval
    # =========================================================================
    ex01_run = BashOperator(
        task_id='ex01_data_retrieval',
        bash_command=f'''
            echo "=== Ex01: Data Retrieval ==="
            echo "Downloading NYC taxi data to MinIO..."

            # Find the JAR (built on host)
            JAR_NAME=$(docker exec {SPARK_MASTER} ls /opt/spark/ | grep -E "ex01.*assembly.*\\.jar$" | head -1)

            if [ -z "$JAR_NAME" ]; then
                echo "ERROR: ex01 JAR not found in spark-master:/opt/spark/"
                echo "Please run: ./run_spark_docker.sh ex01_data_retrieval"
                exit 1
            fi

            echo "Using JAR: $JAR_NAME"
            docker exec {SPARK_MASTER} /opt/spark/bin/spark-submit \
                --class SparkApp \
                --master spark://spark-master:7077 \
                --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
                {SPARK_CONF} \
                /opt/spark/$JAR_NAME
        ''',
    )

    # =========================================================================
    # Ex02: Data Ingestion
    # =========================================================================
    ex02_run = BashOperator(
        task_id='ex02_data_ingestion',
        bash_command=f'''
            echo "=== Ex02: Data Ingestion ==="
            echo "Cleaning and transforming data..."

            JAR_NAME=$(docker exec {SPARK_MASTER} ls /opt/spark/ | grep -E "ex02.*assembly.*\\.jar$" | head -1)

            if [ -z "$JAR_NAME" ]; then
                echo "ERROR: ex02 JAR not found"
                echo "Please run: ./run_spark_docker.sh ex02_data_ingestion"
                exit 1
            fi

            echo "Using JAR: $JAR_NAME"
            docker exec {SPARK_MASTER} /opt/spark/bin/spark-submit \
                --class SparkApp \
                --master spark://spark-master:7077 \
                --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
                {SPARK_CONF} \
                /opt/spark/$JAR_NAME
        ''',
    )

    # =========================================================================
    # Ex03: SQL Table Creation & Data Loading
    # =========================================================================
    with TaskGroup('ex03_dwh_loading', tooltip='Create tables and load to PostgreSQL') as ex03:

        ex03_schema = BashOperator(
            task_id='create_schema',
            bash_command=f'''
                echo "=== Ex03: Creating PostgreSQL schema ==="
                cat {PROJECT_ROOT}/ex03_sql_table_creation/creation.sql | \
                    docker exec -i {POSTGRES_DW} psql -U user_dw -d nyc_data_warehouse
            ''',
        )

        ex03_copy_csv = BashOperator(
            task_id='copy_taxi_zones',
            bash_command=f'''
                echo "Copying taxi zone lookup CSV..."
                docker cp {PROJECT_ROOT}/data/raw/taxi_zone_lookup.csv {POSTGRES_DW}:/tmp/taxi_zone_lookup.csv
            ''',
        )

        ex03_insert = BashOperator(
            task_id='insert_reference_data',
            bash_command=f'''
                echo "=== Inserting reference data ==="
                cat {PROJECT_ROOT}/ex03_sql_table_creation/insertion.sql | \
                    docker exec -i {POSTGRES_DW} psql -U user_dw -d nyc_data_warehouse
            ''',
        )

        ex03_spark = BashOperator(
            task_id='load_fact_table',
            bash_command=f'''
                echo "=== Ex03: Loading fact_trip from MinIO ==="

                JAR_NAME=$(docker exec {SPARK_MASTER} ls /opt/spark/ | grep -E "ex03.*assembly.*\\.jar$" | head -1)

                if [ -z "$JAR_NAME" ]; then
                    echo "ERROR: ex03 JAR not found"
                    echo "Please run: ./run_spark_docker.sh ex03_sql_table_creation IngestToDWH"
                    exit 1
                fi

                echo "Using JAR: $JAR_NAME"
                docker exec {SPARK_MASTER} /opt/spark/bin/spark-submit \
                    --class IngestToDWH \
                    --master spark://spark-master:7077 \
                    --packages {SPARK_PACKAGES} \
                    {SPARK_CONF} \
                    /opt/spark/$JAR_NAME
            ''',
        )

        ex03_dim_date = BashOperator(
            task_id='populate_dim_date',
            bash_command=f'''
                echo "=== Populating dim_date ==="
                cat {PROJECT_ROOT}/ex03_sql_table_creation/populate_dim_date.sql | \
                    docker exec -i {POSTGRES_DW} psql -U user_dw -d nyc_data_warehouse
            ''',
        )

        ex03_agg = BashOperator(
            task_id='run_aggregations',
            bash_command=f'''
                echo "=== Running aggregations ==="
                cat {PROJECT_ROOT}/ex03_sql_table_creation/aggregation.sql | \
                    docker exec -i {POSTGRES_DW} psql -U user_dw -d nyc_data_warehouse
            ''',
        )

        ex03_schema >> ex03_copy_csv >> ex03_insert >> ex03_spark >> ex03_dim_date >> ex03_agg

    # =========================================================================
    # Ex05: ML Training
    # =========================================================================
    ex05_train = BashOperator(
        task_id='ex05_ml_training',
        bash_command=f'''
            echo "=== Ex05: ML Model Training ==="
            cd {PROJECT_ROOT}/ex05_ml_prediction_service

            # Install uv if not present
            pip install --quiet uv 2>/dev/null || true

            # Sync dependencies and run training
            uv sync --quiet
            uv run python src/train.py
        ''',
    )

    # =========================================================================
    # End
    # =========================================================================
    end = EmptyOperator(task_id='end')

    # =========================================================================
    # Pipeline Dependencies
    # =========================================================================
    start >> check_infra >> ex01_run >> ex02_run >> ex03 >> ex05_train >> end


# =============================================================================
# Monthly Refresh DAG (scheduled)
# =============================================================================
with DAG(
    'nyc_taxi_monthly_refresh',
    default_args=default_args,
    description='Monthly refresh of NYC Taxi data with new files',
    schedule_interval='0 2 1 * *',  # 2 AM on the 1st of each month
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nyc-taxi', 'etl', 'monthly', 'scheduled'],
) as dag_monthly:

    notify_start = BashOperator(
        task_id='notify_start',
        bash_command='echo "Monthly NYC Taxi data refresh started at $(date)"',
    )

    # Reuse the same tasks as the main pipeline
    monthly_ex01 = BashOperator(
        task_id='download_new_data',
        bash_command=f'''
            echo "=== Monthly Refresh: Downloading new data ==="
            JAR_NAME=$(docker exec {SPARK_MASTER} ls /opt/spark/ | grep -E "ex01.*assembly.*\\.jar$" | head -1)
            docker exec {SPARK_MASTER} /opt/spark/bin/spark-submit \
                --class SparkApp \
                --master spark://spark-master:7077 \
                --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
                {SPARK_CONF} \
                /opt/spark/$JAR_NAME
        ''',
    )

    monthly_ex02 = BashOperator(
        task_id='process_new_data',
        bash_command=f'''
            echo "=== Monthly Refresh: Processing new data ==="
            JAR_NAME=$(docker exec {SPARK_MASTER} ls /opt/spark/ | grep -E "ex02.*assembly.*\\.jar$" | head -1)
            docker exec {SPARK_MASTER} /opt/spark/bin/spark-submit \
                --class SparkApp \
                --master spark://spark-master:7077 \
                --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
                {SPARK_CONF} \
                /opt/spark/$JAR_NAME
        ''',
    )

    monthly_ex03 = BashOperator(
        task_id='load_to_dwh',
        bash_command=f'''
            echo "=== Monthly Refresh: Loading to DWH ==="
            JAR_NAME=$(docker exec {SPARK_MASTER} ls /opt/spark/ | grep -E "ex03.*assembly.*\\.jar$" | head -1)
            docker exec {SPARK_MASTER} /opt/spark/bin/spark-submit \
                --class IngestToDWH \
                --master spark://spark-master:7077 \
                --packages {SPARK_PACKAGES} \
                {SPARK_CONF} \
                /opt/spark/$JAR_NAME

            # Update aggregations
            cat {PROJECT_ROOT}/ex03_sql_table_creation/populate_dim_date.sql | \
                docker exec -i {POSTGRES_DW} psql -U user_dw -d nyc_data_warehouse
            cat {PROJECT_ROOT}/ex03_sql_table_creation/aggregation.sql | \
                docker exec -i {POSTGRES_DW} psql -U user_dw -d nyc_data_warehouse
        ''',
    )

    monthly_ex05 = BashOperator(
        task_id='retrain_model',
        bash_command=f'''
            echo "=== Monthly Refresh: Retraining ML model ==="
            cd {PROJECT_ROOT}/ex05_ml_prediction_service
            pip install --quiet uv 2>/dev/null || true
            uv sync --quiet
            uv run python src/train.py
        ''',
    )

    notify_end = BashOperator(
        task_id='notify_end',
        bash_command='echo "Monthly NYC Taxi data refresh completed at $(date)"',
    )

    notify_start >> monthly_ex01 >> monthly_ex02 >> monthly_ex03 >> monthly_ex05 >> notify_end
