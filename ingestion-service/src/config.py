import os

# MinIO / S3
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
RAW_BUCKET = os.getenv("RAW_BUCKET", "raw")

# Dataset
DATASET_LOCAL_PATH = os.getenv("DATASET_LOCAL_PATH", "/data/nyc_taxi.csv")
RAW_OBJECT_KEY = os.getenv("RAW_OBJECT_KEY", "nyc_taxi_trip_duration/nyc_taxi.csv")

# Postgres
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "metadata")
PG_USER = os.getenv("POSTGRES_USER", "de_user")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "de_pass")
