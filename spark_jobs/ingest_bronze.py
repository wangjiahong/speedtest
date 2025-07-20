import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurations (adjust as needed)
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'speedtest_raw')
ICEBERG_CATALOG = os.getenv('ICEBERG_CATALOG', 'iceberg_catalog')
ICEBERG_DB = os.getenv('ICEBERG_DB', 'bronze')
ICEBERG_TABLE = os.getenv('ICEBERG_TABLE', 'speedtest_raw')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'speedtest')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'iceberg')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'iceberg')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'iceberg')

logger.info("====> === Starting IngestKafkaToBronzeIceberg Job ===")
logger.info(f"====> Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
logger.info(f"====> Kafka Topic: {KAFKA_TOPIC}")
logger.info(f"====> Iceberg Catalog: {ICEBERG_CATALOG}")
logger.info(f"====> Iceberg Database: {ICEBERG_DB}")
logger.info(f"====> Iceberg Table: {ICEBERG_TABLE}")

# Define the JSON schema (matches the data generator)
speedtest_schema = StructType([
    StructField('timestamp', LongType(), False),
    StructField('user_id', StringType(), False),
    StructField('upload_speed', FloatType(), False),
    StructField('download_speed', FloatType(), False),
    StructField('latency', FloatType(), False),
    StructField('location', StringType(), False)
])

logger.info("====> JSON Schema defined successfully")

# Start Spark session with Iceberg and S3/MinIO support
logger.info("====> Initializing Spark session...")
spark = SparkSession.builder \
    .appName('IngestKafkaToBronzeIceberg') \
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

logger.info("====> Spark session created successfully")

# Record start time for metrics
start_time = time.time()

# Read from Kafka
logger.info(f"====> Reading from Kafka topic: {KAFKA_TOPIC}")
kafka_df = spark.read \
    .format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS) \
    .option('subscribe', KAFKA_TOPIC) \
    .option('startingOffsets', 'earliest') \
    .load()

logger.info(f"====> Kafka DataFrame count: {kafka_df.count()}")
logger.info("====> Kafka DataFrame schema:")
kafka_df.printSchema()

# Show sample Kafka data
logger.info("====> Sample Kafka data (first 5 rows):")
kafka_df.show(5, truncate=False)
logger.info('====> end of kafka_df')

# Parse JSON value (plain JSON, not Avro)
logger.info("====> Parsing JSON data from Kafka...")
df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col('json_str'), speedtest_schema).alias('data')) \
    .select('data.*')

logger.info(f"====> Parsed DataFrame count: {df.count()}")
logger.info("====> Parsed DataFrame schema:")
df.printSchema()

# Show sample parsed data
logger.info("====> Sample parsed data (first 5 rows):")
df.show(5, truncate=False)

# Add partition column (date) - fix timestamp conversion
logger.info("====> Adding date partition column...")
df = df.withColumn('date', to_date(col('timestamp').cast('timestamp')))

logger.info(f"====> DataFrame with date column count: {df.count()}")
logger.info("====> DataFrame with date column schema:")
df.printSchema()

# Show sample data with date column
logger.info("====> Sample data with date column (first 5 rows):")
df.show(5, truncate=False)

# Show date distribution
logger.info("====> Date distribution:")
df.groupBy('date').count().orderBy('date').show()

# Create Iceberg table if not exists
logger.info(f"====> Creating Iceberg table: {ICEBERG_CATALOG}.{ICEBERG_DB}.{ICEBERG_TABLE}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_DB}.{ICEBERG_TABLE} (
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

logger.info("====> Iceberg table created/verified successfully")

# Write to Iceberg (append mode)
final_count = df.count()
logger.info(f"====> Writing {final_count} rows to Iceberg table...")
logger.info(df.show(100))
logger.info('====> end of df')

df.writeTo(f"{ICEBERG_CATALOG}.{ICEBERG_DB}.{ICEBERG_TABLE}").append()

# Calculate and log metrics
end_time = time.time()
processing_time = end_time - start_time
logger.info(f"====> === Job Metrics ===")
logger.info(f"====> Total rows processed: {final_count}")
logger.info(f"====> Processing time: {processing_time:.2f} seconds")
logger.info(f"====> Processing rate: {final_count/processing_time:.2f} rows/second")
logger.info(f"====> === IngestKafkaToBronzeIceberg Job Completed Successfully ===")

spark.stop() 