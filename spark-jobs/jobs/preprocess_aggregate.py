import os
from datetime import datetime, timezone
import boto3

from pyspark.sql import SparkSession, functions as F, types as T


# ---------- Config (env) ----------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")

RAW_BUCKET = os.getenv("RAW_BUCKET", "raw")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET", "processed")

RAW_PREFIX = os.getenv("RAW_PREFIX", "nyc_taxi_trip_duration/")
PROCESSED_PREFIX = os.getenv("PROCESSED_PREFIX", "nyc_taxi_trip_duration/quarterly_aggregates/")


def s3_client():
    # MinIO is S3-compatible; use boto3 with endpoint_url
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def find_latest_raw_key(client) -> str:
    # list objects under RAW_PREFIX and pick latest by LastModified
    paginator = client.get_paginator("list_objects_v2")
    latest = None

    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=RAW_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # skip "folders"
            if key.endswith("/"):
                continue
            if (latest is None) or (obj["LastModified"] > latest["LastModified"]):
                latest = obj

    if latest is None:
        raise RuntimeError(f"No objects found in s3://{RAW_BUCKET}/{RAW_PREFIX}")

    return latest["Key"]


def upload_directory(client, local_dir: str, bucket: str, prefix: str):
    # Upload all files under local_dir to bucket/prefix keeping relative paths
    for root, _, files in os.walk(local_dir):
        for fname in files:
            local_path = os.path.join(root, fname)
            rel_path = os.path.relpath(local_path, local_dir).replace("\\", "/")
            key = f"{prefix.rstrip('/')}/{rel_path}"
            client.upload_file(local_path, bucket, key)


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("nyc_taxi_preprocess_quarterly_agg")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    return spark


def main():
    print("STARTED: spark_preprocess_quarterly_agg")

    client = s3_client()
    spark = build_spark()

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")

    try:
        latest_key = find_latest_raw_key(client)
        print(f"INFO: Latest raw object = s3://{RAW_BUCKET}/{latest_key}")

        # Download latest raw CSV to local file inside container
        local_csv = "/shared/nyc_latest.csv"
        client.download_file(RAW_BUCKET, latest_key, local_csv)
        print(f"INFO: Downloaded to {local_csv}")

        # Read local CSV with Spark
        schema = T.StructType([
            T.StructField("id", T.StringType(), True),
            T.StructField("vendor_id", T.IntegerType(), True),
            T.StructField("pickup_datetime", T.StringType(), True),
            T.StructField("dropoff_datetime", T.StringType(), True),
            T.StructField("passenger_count", T.IntegerType(), True),
            T.StructField("pickup_longitude", T.DoubleType(), True),
            T.StructField("pickup_latitude", T.DoubleType(), True),
            T.StructField("dropoff_longitude", T.DoubleType(), True),
            T.StructField("dropoff_latitude", T.DoubleType(), True),
            T.StructField("store_and_fwd_flag", T.StringType(), True),
            T.StructField("trip_duration", T.IntegerType(), True),
        ])

        df = (
            spark.read
            .option("header", "true")
            .schema(schema)
            .csv(local_csv)
        )

        records_in = df.count()
        print(f"INFO: records_in = {records_in}")

        # Clean + validate
        df = (
            df.withColumn("pickup_ts", F.to_timestamp("pickup_datetime"))
              .filter(F.col("pickup_ts").isNotNull())
              .filter(F.col("trip_duration").isNotNull())
              .filter(F.col("trip_duration") > 0)
              .dropDuplicates(["id"])
              .withColumn("year", F.year("pickup_ts"))
              .withColumn("quarter", F.quarter("pickup_ts"))
        )

        # Aggregate quarterly
        agg = (
            df.groupBy("year", "quarter")
              .agg(
                  F.count("*").alias("trip_count"),
                  F.avg("trip_duration").alias("avg_trip_duration_seconds"),
                  F.expr("percentile_approx(trip_duration, 0.5)").alias("median_trip_duration_seconds"),
              )
              .orderBy("year", "quarter")
        )

        records_out = agg.count()
        print(f"INFO: records_out = {records_out}")

        # Write parquet locally
        local_out = "/shared/out_parquet"
        (agg.write
            .mode("overwrite")
            .partitionBy("year", "quarter")
            .parquet(local_out))
        print(f"DONE: Parquet written locally at {local_out}")

        # Upload parquet folder to MinIO processed bucket
        out_prefix = f"{PROCESSED_PREFIX.rstrip('/')}/run={ts}"
        upload_directory(client, local_out, PROCESSED_BUCKET, out_prefix)

        print(f"DONE: Uploaded parquet to s3://{PROCESSED_BUCKET}/{out_prefix}")
        print("DONE: spark_preprocess_quarterly_agg completed successfully")

    except Exception as e:
        print(f"FAILED: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
