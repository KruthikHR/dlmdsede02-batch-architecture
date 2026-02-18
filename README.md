# dlmdsede02-batch-architecture
Docker-based batch data processing architecture using Spark and MinIO

Project Overview

This project implements a Docker-based batch processing pipeline using Apache Spark to process NYC Taxi Trip Duration data.

The pipeline reads raw CSV data from object storage (MinIO – S3 compatible), performs data cleaning and quarterly aggregation, and writes the processed output back to object storage in Parquet format, and loads final aggregates into PostgreSQL for analytical querying.

Architecture

The system uses a containerized architecture:
   1. Docker Compose
   2. Apache Spark (Master + Worker)
   3. MinIO (S3-compatible storage)
   4. PostgreSQL (Metadata + Analytics storage)
   5. Ingestion Service (Python  + boto3)
   6. Schedule Service

Data Flow:
Raw CSV → MinIO (raw bucket) → Spark Processing → Parquet Output → MinIO (processed bucket) → Spark Load Job → PostgreSQL (analytics table)

Tech Stack

   1. Apache Spark 3.3.0
   2. Hadoop 3.3
   3. Docker & Docker Compose
   4. MinIO (S3-compatible object storage)
   5. Python (PySpark)
   6. boto3
   7. PostgreSQL 13
   8. JDBC (Postgres Driver)

Project Structure

dlmdsede02-batch-architecture/
│
├── ingestion-service/
│   └── ingestion_minio_raw.py
│
├── spark-jobs/
│   ├── jobs/
│   │   ├── preprocess_aggregate.py
│   │   └── load_to_postgres.py
│   └── configs/
│       └── spark-defaults.conf
│
├── postgres/
│   └── init.sql
│
├── scheduler/
│   └── scheduler.py
│
├── spark-shared/
│
├── docker-compose.yml
├── .env.example
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

4. Load to PostgreSQL
   * Reads aggregated Parquet output
   * Loads into PostgreSQL table: nyc_quarterly_aggregates
    
       
How to Run

1. Start containers:
   docker compose up -d
2. Run Spark job:
   docker exec -it spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /spark-apps/preprocess_aggregate.py
3. Load Aggregates into PostgreSQL:
   docker exec -it spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.7.3 \
  /spark-apps/load_to_postgres.py

  
