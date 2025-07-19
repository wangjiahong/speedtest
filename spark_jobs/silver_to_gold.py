import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour, avg, from_unixtime

# Configurations (adjust as needed)
ICEBERG_CATALOG = os.getenv('ICEBERG_CATALOG', 'iceberg_catalog')
SILVER_DB = os.getenv('SILVER_DB', 'silver')
SILVER_TABLE = os.getenv('SILVER_TABLE', 'speedtest_clean')
GOLD_DB = os.getenv('GOLD_DB', 'gold')
GOLD_TABLE = os.getenv('GOLD_TABLE', 'speedtest_agg')
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
    .appName('SilverToGoldIceberg') \
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

# Read from silver Iceberg table
df = spark.read.format('iceberg').load(f'{ICEBERG_CATALOG}.{SILVER_DB}.{SILVER_TABLE}')

# Add hour column for aggregation
df = df.withColumn('hour', hour(col('timestamp').cast('timestamp')))

# Aggregate: average upload/download speeds and latency per location per hour
df_agg = df.groupBy('date', 'location', 'hour').agg(
    avg('upload_speed').alias('avg_upload_speed'),
    avg('download_speed').alias('avg_download_speed'),
    avg('latency').alias('avg_latency')
)

# Create gold Iceberg table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{GOLD_DB}.{GOLD_TABLE} (
    date DATE,
    location STRING,
    hour INT,
    avg_upload_speed DOUBLE,
    avg_download_speed DOUBLE,
    avg_latency DOUBLE
) PARTITIONED BY (date, location)
TBLPROPERTIES ('format-version'='2')
""")

# Write to gold Iceberg table (overwrite for idempotency)
df_agg.writeTo(f"{ICEBERG_CATALOG}.{GOLD_DB}.{GOLD_TABLE}").overwritePartitions()

spark.stop() 