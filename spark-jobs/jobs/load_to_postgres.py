import os
from pyspark.sql import SparkSession

def main():
    spark = (
        SparkSession.builder
        .appName("nyc_taxi_load_quarterly_aggregates_to_postgres")
        .getOrCreate()
    )

    # Read aggregated parquet produced by preprocess_aggregate.py
    parquet_path = os.environ.get("AGG_PARQUET_PATH", "/shared/out_parquet")
    df = spark.read.parquet(parquet_path)

    print(f"INFO: Loaded parquet from {parquet_path}")
    print(f"INFO: rows={df.count()}")
    df.printSchema()

    # Postgres connection (from docker-compose env)
    pg_host = os.environ.get("POSTGRES_HOST", "postgres")
    pg_port = os.environ.get("POSTGRES_PORT", "5432")
    pg_db = os.environ.get("POSTGRES_DB", "metadata")
    pg_user = os.environ.get("POSTGRES_USER", "de_user")
    pg_pass = os.environ.get("POSTGRES_PASSWORD", "de_pass")

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    table = os.environ.get("POSTGRES_TABLE", "nyc_quarterly_aggregates")

    props = {
        "user": pg_user,
        "password": pg_pass,
        "driver": "org.postgresql.Driver",
    }

    # Write to Postgres (overwrite for now; later we can do append + run_id)
    (
        df.write
        .mode("overwrite")
        .jdbc(url=jdbc_url, table=table, properties=props)
    )

    print(f"DONE: wrote to Postgres table {table} at {jdbc_url}")

    spark.stop()

if __name__ == "__main__":
    main()
