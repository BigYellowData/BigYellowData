FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    docker.io \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    apache-airflow-providers-docker \
    apache-airflow-providers-postgres \
    minio \
    pandas \
    python-dotenv
