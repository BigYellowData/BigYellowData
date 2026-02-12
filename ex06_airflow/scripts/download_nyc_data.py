"""
NYC TLC Yellow Taxi Data Downloader

Downloads new monthly parquet files from the NYC TLC website
and uploads them directly to MinIO, skipping files already present.

Used by the Airflow monthly refresh DAG to automatically fetch
the latest available data without manual intervention.
"""
import os
import logging
import tempfile
from datetime import date
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from minio import Minio

logger = logging.getLogger(__name__)

# NYC TLC data URL pattern
TLC_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_{year}-{month:02d}.parquet"
)

# MinIO configuration
MINIO_BUCKET = "nyctaxiproject"
MINIO_RAW_PREFIX = "nyc_raw/"

# How far back to check for missing months
LOOKBACK_MONTHS = 12
# TLC publishes with ~2 month delay
MIN_DELAY_MONTHS = 2


def get_minio_client():
    """Create a MinIO client from environment variables."""
    raw_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    endpoint = raw_endpoint.replace("http://", "").replace("https://", "")
    access_key = os.getenv("MINIO_ROOT_USER", "minio")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minio123")

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )


def subtract_months(year, month, n):
    """Subtract n months from a (year, month) pair."""
    month -= n
    while month <= 0:
        month += 12
        year -= 1
    return year, month


def get_existing_months_in_minio(client):
    """
    List objects in nyc_raw/ and extract YYYY-MM months already present.

    Only detects files uploaded by this script (named
    yellow_tripdata_YYYY-MM.parquet), not Spark-written partitions.

    Returns a set of (year, month) tuples.
    """
    existing = set()
    try:
        objects = client.list_objects(
            MINIO_BUCKET, prefix=MINIO_RAW_PREFIX, recursive=True
        )
        for obj in objects:
            name = obj.object_name
            if "yellow_tripdata_" in name:
                try:
                    part = name.split("yellow_tripdata_")[1][:7]
                    year, month = int(part[:4]), int(part[5:7])
                    existing.add((year, month))
                except (IndexError, ValueError):
                    continue
    except Exception as e:
        logger.warning(f"Could not list MinIO objects: {e}")
    return existing


def get_candidate_months():
    """
    Compute the list of (year, month) tuples to check for download.

    Goes from (today - MIN_DELAY_MONTHS) backwards for LOOKBACK_MONTHS.
    """
    today = date.today()
    candidates = []
    for i in range(MIN_DELAY_MONTHS, MIN_DELAY_MONTHS + LOOKBACK_MONTHS):
        y, m = subtract_months(today.year, today.month, i)
        candidates.append((y, m))
    return candidates


def check_url_exists(url):
    """Send an HTTP HEAD request to verify the URL is available."""
    try:
        req = Request(url, method="HEAD")
        req.add_header("User-Agent", "BigYellowData-Pipeline/1.0")
        response = urlopen(req, timeout=15)
        return response.status == 200
    except (HTTPError, URLError, OSError):
        return False


def download_and_upload(year, month, client):
    """
    Download a single month's parquet file from TLC and upload to MinIO.

    Uses a temporary file to avoid loading the entire file in memory.

    Returns True on success, False on failure.
    """
    url = TLC_URL_TEMPLATE.format(year=year, month=month)
    object_name = f"{MINIO_RAW_PREFIX}yellow_tripdata_{year}-{month:02d}.parquet"

    logger.info(f"Downloading {url} ...")

    try:
        req = Request(url)
        req.add_header("User-Agent", "BigYellowData-Pipeline/1.0")
        response = urlopen(req, timeout=300)

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=True) as tmp:
            total_bytes = 0
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                tmp.write(chunk)
                total_bytes += len(chunk)
            tmp.flush()

            size_mb = total_bytes / (1024 * 1024)
            logger.info(f"Downloaded {size_mb:.1f} MB -> uploading to MinIO...")

            client.fput_object(
                MINIO_BUCKET,
                object_name,
                tmp.name,
                content_type="application/octet-stream",
            )

        logger.info(f"Uploaded to MinIO: {object_name}")
        return True

    except Exception as e:
        logger.error(f"Failed to download/upload {year}-{month:02d}: {e}")
        return False


def download_new_taxi_data(**kwargs):
    """
    Main entry point for the Airflow PythonOperator.

    1. Connect to MinIO and list existing months
    2. Compute candidate months to check
    3. For each missing month, check TLC availability and download
    4. Return list of new months (pushed to XCom automatically)

    Returns:
        list[str]: Newly downloaded month strings, e.g. ["2025-11", "2025-12"]
    """
    logging.basicConfig(level=logging.INFO)

    client = get_minio_client()

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        logger.info(f"Created bucket: {MINIO_BUCKET}")

    existing = get_existing_months_in_minio(client)
    logger.info(
        f"Months already in MinIO: "
        f"{sorted(f'{y}-{m:02d}' for y, m in existing)}"
    )

    candidates = get_candidate_months()
    missing = [(y, m) for y, m in candidates if (y, m) not in existing]

    if not missing:
        logger.info("No new months to download. MinIO is up to date.")
        return []

    logger.info(
        f"Missing months to check: "
        f"{[f'{y}-{m:02d}' for y, m in missing]}"
    )

    downloaded = []
    for year, month in missing:
        url = TLC_URL_TEMPLATE.format(year=year, month=month)
        if check_url_exists(url):
            if download_and_upload(year, month, client):
                downloaded.append(f"{year}-{month:02d}")
        else:
            logger.info(f"{year}-{month:02d}: not yet available on TLC")

    if downloaded:
        logger.info(f"Successfully downloaded new months: {downloaded}")
    else:
        logger.info("No new data available from TLC at this time.")

    return downloaded
