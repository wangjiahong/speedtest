import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Configurations (adjust as needed)
ICEBERG_CATALOG = os.getenv('ICEBERG_CATALOG', 'iceberg_catalog')
BRONZE_DB = os.getenv('BRONZE_DB', 'bronze')
BRONZE_TABLE = os.getenv('BRONZE_TABLE', 'speedtest_raw')
SILVER_DB = os.getenv('SILVER_DB', 'silver')
SILVER_TABLE = os.getenv('SILVER_TABLE', 'speedtest_clean')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'speedtest')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'iceberg')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'iceberg')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'iceberg')

# Start Spark session with Iceberg and S3/MinIO support
spark = SparkSession.builder \
    .appName('BronzeToSilverIceberg') \
    .config('spark.sql.catalog.' + ICEBERG_CATALOG, 'org.apache.iceberg.spark.SparkCatalog') \
    .config('spark.sql.catalog.' + ICEBERG_CATALOG + '.catalog-impl', 'org.apache.iceberg.jdbc.JdbcCatalog') \
    .config('spark.sql.catalog.' + ICEBERG_CATALOG + '.uri', f'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}') \
    .config('spark.sql.catalog.' + ICEBERG_CATALOG + '.jdbc.user', POSTGRES_USER) \
    .config('spark.sql.catalog.' + ICEBERG_CATALOG + '.jdbc.password', POSTGRES_PASSWORD) \
    .config('spark.sql.catalog.' + ICEBERG_CATALOG + '.warehouse', f's3a://{MINIO_BUCKET}/warehouse') \
    .config('spark.hadoop.fs.s3a.endpoint', MINIO_ENDPOINT) \
    .config('spark.hadoop.fs.s3a.access.key', MINIO_ACCESS_KEY) \
    .config('spark.hadoop.fs.s3a.secret.key', MINIO_SECRET_KEY) \
    .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    .getOrCreate()

# Read from bronze Iceberg table
df = spark.read.format('iceberg').load(f'{ICEBERG_CATALOG}.{BRONZE_DB}.{BRONZE_TABLE}')

# Cleaning: drop records with nulls
df_clean = df.dropna()

# Deduplicate by user_id and timestamp
df_dedup = df_clean.dropDuplicates(['user_id', 'timestamp'])

# Add partition column (date) if not present
df_final = df_dedup
if 'date' not in df_final.columns:
    df_final = df_final.withColumn('date', to_date(col('timestamp').cast('timestamp')))

# Create silver Iceberg table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{SILVER_DB}.{SILVER_TABLE} (
    timestamp LONG,
    user_id STRING,
    upload_speed FLOAT,
    download_speed FLOAT,
    latency FLOAT,
    location STRING,
    date DATE
) PARTITIONED BY (date)
TBLPROPERTIES ('format-version'='2')
""")

# Write to silver Iceberg table (overwrite for idempotency)
df_final.writeTo(f"{ICEBERG_CATALOG}.{SILVER_DB}.{SILVER_TABLE}").overwritePartitions()

spark.stop() 