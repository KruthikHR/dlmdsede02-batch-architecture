import os
from datetime import datetime, timezone

import boto3
import psycopg2

from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    RAW_BUCKET,
    DATASET_LOCAL_PATH,
    RAW_OBJECT_KEY,
    PG_HOST,
    PG_PORT,
    PG_DB,
    PG_USER,
    PG_PASSWORD,
)


def pg_connect():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )


def log_run_start(conn, job_name: str, input_path: str, output_path: str | None = None) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs (job_name, status, start_time, input_path, output_path, message)
            VALUES (%s, 'STARTED', NOW(), %s, %s, %s)
            RETURNING id
            """,
            (job_name, input_path, output_path, "Ingestion started"),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def log_run_end(conn, run_id: int, status: str, records_in: int | None = None, message: str = ""):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_runs
            SET status=%s, end_time=NOW(), records_in=%s, message=%s
            WHERE id=%s
            """,
            (status, records_in, message, run_id),
        )
    conn.commit()


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def ensure_bucket(client, bucket: str):
    # bucket usually already created by minio-init; this is extra safety
    try:
        client.head_bucket(Bucket=bucket)
    except Exception:
        client.create_bucket(Bucket=bucket)


def estimate_rows(local_path: str) -> int:
    # fast-ish line count for CSV (minus header). Works for basic CSV files.
    try:
        with open(local_path, "rb") as f:
            lines = sum(1 for _ in f)
        return max(lines - 1, 0)
    except Exception:
        return 0


def main():
    job_name = "ingestion_minio_raw"
    input_path = DATASET_LOCAL_PATH
    output_path = f"s3://{RAW_BUCKET}/{RAW_OBJECT_KEY}"

    if not os.path.exists(DATASET_LOCAL_PATH):
        raise FileNotFoundError(
            f"Dataset not found at {DATASET_LOCAL_PATH}. "
            f"Put your CSV into ./data and set DATASET_LOCAL_PATH if needed."
        )

    conn = pg_connect()
    run_id = log_run_start(conn, job_name=job_name, input_path=input_path, output_path=output_path)

    try:
        client = s3_client()
        ensure_bucket(client, RAW_BUCKET)

        # Add a timestamped folder for reproducibility
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
        object_key = RAW_OBJECT_KEY.replace(".csv", f"_{ts}.csv")

        client.upload_file(DATASET_LOCAL_PATH, RAW_BUCKET, object_key)

        rows = estimate_rows(DATASET_LOCAL_PATH)
        log_run_end(conn, run_id, status="SUCCESS", records_in=rows,
                    message=f"Uploaded to s3://{RAW_BUCKET}/{object_key}")
        print(f"Done Uploaded {DATASET_LOCAL_PATH} → s3://{RAW_BUCKET}/{object_key} (rows≈{rows})")

    except Exception as e:
        log_run_end(conn, run_id, status="FAILED", message=str(e))
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()
