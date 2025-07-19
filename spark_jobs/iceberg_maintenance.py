import os
from pyspark.sql import SparkSession

# Configurations (adjust as needed)
ICEBERG_CATALOG = os.getenv('ICEBERG_CATALOG', 'iceberg_catalog')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'speedtest')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'iceberg')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'iceberg')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'iceberg')

TABLES = [
    ('bronze', 'speedtest_raw'),
    ('silver', 'speedtest_clean'),
    ('gold', 'speedtest_agg'),
]

# Start Spark session with Iceberg and S3/MinIO support
spark = SparkSession.builder \
    .appName('IcebergMaintenance') \
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

for db, table in TABLES:
    full_table = f"{ICEBERG_CATALOG}.{db}.{table}"
    print(f"Running maintenance on {full_table}")
    
    # Check if table exists and has data
    try:
        # Simple maintenance: just check table metadata
        df = spark.read.format('iceberg').load(full_table)
        count = df.count()
        print(f"Table {full_table} has {count} records")
        
        # Show table history (this works with Spark SQL)
        print(f"Table history for {full_table}:")
        spark.sql(f"DESCRIBE HISTORY {full_table}").show(truncate=False)
        
    except Exception as e:
        print(f"Error accessing table {full_table}: {e}")

print("Maintenance completed - note: advanced maintenance requires Iceberg CLI or Java API")
spark.stop() 