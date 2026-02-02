#!/bin/bash
# Script to run BigYellowData exercises from Airflow
# This script is executed inside the Airflow container and uses docker exec
# to interact with other containers

set -e

EXERCISE="$1"
PROJECT_ROOT="/project"

echo "[Airflow] Running exercise: $EXERCISE"

case "$EXERCISE" in
    "ex01")
        echo "[Airflow] Ex01: Data Retrieval"
        # The JAR should be pre-built and copied to spark-master
        docker exec spark-master /opt/spark/bin/spark-submit \
            --class SparkApp \
            --master spark://spark-master:7077 \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
            /opt/spark/ex01-assembly.jar
        ;;

    "ex02")
        echo "[Airflow] Ex02: Data Ingestion"
        docker exec spark-master /opt/spark/bin/spark-submit \
            --class SparkApp \
            --master spark://spark-master:7077 \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
            /opt/spark/ex02-assembly.jar
        ;;

    "ex03-sql")
        echo "[Airflow] Ex03: SQL Table Creation"
        docker exec -i postgres-dw psql -U user_dw -d nyc_data_warehouse < $PROJECT_ROOT/ex03_sql_table_creation/creation.sql
        docker cp $PROJECT_ROOT/data/raw/taxi_zone_lookup.csv postgres-dw:/tmp/taxi_zone_lookup.csv
        docker exec -i postgres-dw psql -U user_dw -d nyc_data_warehouse < $PROJECT_ROOT/ex03_sql_table_creation/insertion.sql
        ;;

    "ex03-spark")
        echo "[Airflow] Ex03: Spark Ingestion to DWH"
        docker exec spark-master /opt/spark/bin/spark-submit \
            --class IngestToDWH \
            --master spark://spark-master:7077 \
            --packages org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
            /opt/spark/ex03-assembly.jar
        ;;

    "ex03-aggregation")
        echo "[Airflow] Ex03: Aggregations"
        docker exec -i postgres-dw psql -U user_dw -d nyc_data_warehouse < $PROJECT_ROOT/ex03_sql_table_creation/populate_dim_date.sql
        docker exec -i postgres-dw psql -U user_dw -d nyc_data_warehouse < $PROJECT_ROOT/ex03_sql_table_creation/aggregation.sql
        ;;

    "ex05")
        echo "[Airflow] Ex05: ML Training"
        cd $PROJECT_ROOT/ex05_ml_prediction_service
        python -m pip install --quiet uv 2>/dev/null || true
        uv sync
        uv run python src/train.py
        ;;

    *)
        echo "Unknown exercise: $EXERCISE"
        echo "Usage: $0 [ex01|ex02|ex03-sql|ex03-spark|ex03-aggregation|ex05]"
        exit 1
        ;;
esac

echo "[Airflow] Exercise $EXERCISE completed successfully!"
