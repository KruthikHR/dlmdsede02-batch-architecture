# dlmdsede02-batch-architecture
Docker-based batch data processing architecture using Spark and MinIO

Project Overview

This project implements a Docker-based batch processing pipeline using Apache Spark to process NYC Taxi Trip Duration data.

The pipeline reads raw CSV data from object storage (MinIO – S3 compatible), performs data cleaning and quarterly aggregation, and writes the processed output back to object storage in Parquet format.

Architecture

The system uses a containerized architecture:
   1. Docker Compose
   2. Apache Spark (Master + Worker)
   3. MinIO (S3-compatible storage)
   4. PostgreSQL (running, not used in Phase 2)

Data Flow:
Raw CSV → MinIO (raw bucket) → Spark Processing → Parquet Output → MinIO (processed bucket)

Tech Stack

   1. Apache Spark 3.3.0
   2. Hadoop 3.3
   3. Docker & Docker Compose
   4. MinIO (S3-compatible object storage)
   5. Python (PySpark)
   6. boto3

Project Structure

dlmdsede02-batch-architecture/
│
├── spark-apps/
│   └── preprocess_aggregate.py
│
├── docker-compose.yml
├── master.sh
├── worker.sh
└── README.md

Processing Logic

1. The Spark job performs:

    * Reads latest raw CSV from MinIO
    * Filters invalid trip durations
    * Extracts year and quarter from pickup_datetime

2. Computes:

   * trip_count
   * avg_trip_duration_seconds
   * median_trip_duration_seconds
     
3. Writes partitioned Parquet output:
     * year
     * quarter
       
How to Run

Start containers:
   docker compose up -d
Run Spark job:
   docker exec -it spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /spark-apps/preprocess_aggregate.py
