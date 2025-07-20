# Data Pipeline Makefile
# Medallion Architecture with Kafka, Spark, Iceberg, Trino, and Airflow

.PHONY: help build up down restart logs clean status test data-generate spark-run airflow-trigger grafana prometheus kafka-check trino-query

# Default target
help:
	@echo "🚀 SpeedTest Data Pipeline - Available Commands:"
	@echo ""
	@echo "📦 Infrastructure:"
	@echo "  build          - Build all Docker images"
	@echo "  up             - Start all services"
	@echo "  down           - Stop all services"
	@echo "  restart        - Restart all services"
	@echo "  clean          - Stop and remove all containers, networks, volumes"
	@echo "  status         - Show status of all services"
	@echo ""
	@echo "📊 Monitoring:"
	@echo "  logs           - Show logs from all services"
	@echo "  grafana        - Open Grafana dashboard"
	@echo "  prometheus     - Open Prometheus metrics"
	@echo "  kafdrop        - Open Kafdrop (Kafka UI)"
	@echo ""
	@echo "🔄 Data Operations:"
	@echo "  data-generate  - Start data generation"
	@echo "  data-stop      - Stop data generation"
	@echo "  spark-run      - Run Spark job manually"
	@echo "  airflow-trigger - Trigger Airflow DAG"
	@echo ""
	@echo "🔍 Debugging:"
	@echo "  kafka-check    - Check Kafka messages"
	@echo "  trino-query    - Run Trino query"
	@echo "  test           - Run basic connectivity tests"
	@echo ""
	@echo "🧹 Maintenance:"
	@echo "  kafka-clear    - Clear Kafka topic"
	@echo "  minio-clear    - Clear MinIO data"
	@echo "  postgres-clear - Clear PostgreSQL data"

# Infrastructure Commands
build:
	@echo "🔨 Building Docker images..."
	docker-compose build

up:
	@echo "🚀 Starting all services..."
	docker-compose up -d
	@echo "✅ Services started. Check status with: make status"

down:
	@echo "🛑 Stopping all services..."
	docker-compose down

restart:
	@echo "🔄 Restarting all services..."
	docker-compose restart

clean:
	@echo "🧹 Cleaning up all containers, networks, and volumes..."
	docker-compose down -v
	docker system prune -f

status:
	@echo "📊 Service Status:"
	docker-compose ps

rebuildandup:
	@echo "🔨 Rebuilding and starting all services..."
	make build
	make up

# Monitoring Commands
logs:
	@echo "📋 Showing logs from all services..."
	docker-compose logs -f

grafana:
	@echo "📈 Opening Grafana dashboard..."
	@echo "URL: http://localhost:3000"
	@echo "Username: admin"
	@echo "Password: admin"
	@start http://localhost:3000

grafana-setup:
	@echo "🔧 Setting up Grafana with Prometheus data source and dashboard..."
	python setup_grafana.py

prometheus:
	@echo "📊 Opening Prometheus metrics..."
	@echo "URL: http://localhost:9090"
	@start http://localhost:9090

kafdrop:
	@echo "📨 Opening Kafdrop (Kafka UI)..."
	@echo "URL: http://localhost:9002"
	@start http://localhost:9002

airflow:
	@echo "🛠️ Opening Airflow UI..."
	@echo "URL: http://localhost:8082"
	@echo "Username: admin"
	@echo "Password: admin"
	@start http://localhost:8082

# Data Operations
data-generate:
	@echo "📊 Starting data generation..."
	docker-compose up -d data-gen

data-stop:
	@echo "🛑 Stopping data generation..."
	docker stop speedtest_c-data-gen-1

spark-run:
	@echo "⚡ Running Spark job manually..."
	docker exec -it speedtest_c-spark-1 spark-submit \
		--master spark://spark:7077 \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,org.postgresql:postgresql:42.6.0 \
		--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
		--conf spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog \
		--conf spark.sql.catalog.iceberg_catalog.type=jdbc \
		--conf spark.sql.catalog.iceberg_catalog.uri=jdbc:postgresql://postgres:5432/iceberg \
		--conf spark.sql.catalog.iceberg_catalog.jdbc.user=iceberg \
		--conf spark.sql.catalog.iceberg_catalog.jdbc.password=iceberg \
		--conf spark.sql.catalog.iceberg_catalog.warehouse=s3a://speedtest/warehouse \
		--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
		--conf spark.hadoop.fs.s3a.access.key=minioadmin \
		--conf spark.hadoop.fs.s3a.secret.key=minioadmin \
		--conf spark.hadoop.fs.s3a.path.style.access=true \
		--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
		/opt/spark-apps/ingest_bronze.py

airflow-trigger:
	@echo "🔄 Triggering Airflow DAG..."
	@echo "Please go to http://localhost:8082 and trigger the medallion_pipeline DAG manually"

# Debugging Commands
kafka-check:
	@echo "📨 Checking Kafka messages..."
	docker exec -it speedtest_c-kafka-1 kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic speedtest_raw \
		--from-beginning \
		--max-messages 5

trino-query:
	@echo "🔍 Running Trino query..."
	docker exec -it speedtest_c-trino-1 trino \
		--server localhost:8080 \
		--catalog iceberg_catalog \
		--schema bronze \
		--execute "SELECT COUNT(*) FROM speedtest_raw;"

test:
	@echo "🧪 Running connectivity tests..."
	@echo "Testing Kafka..."
	docker exec -it speedtest_c-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
	@echo "Testing Trino..."
	docker exec -it speedtest_c-trino-1 trino --server localhost:8080 --catalog iceberg_catalog --schema bronze --execute "SHOW TABLES;"
	@echo "Testing MinIO..."
	docker exec -it speedtest_c-minio-1 mc ls speedtest/warehouse/ || echo "MinIO not ready yet"

# Maintenance Commands
kafka-clear:
	@echo "🗑️ Clearing Kafka topic..."
	docker exec -it speedtest_c-kafka-1 kafka-topics --bootstrap-server localhost:9092 --delete --topic speedtest_raw
	docker exec -it speedtest_c-kafka-1 kafka-topics --bootstrap-server localhost:9092 --create --topic speedtest_raw --partitions 3 --replication-factor 1
	@echo "✅ Kafka topic cleared and recreated"

minio-clear:
	@echo "🗑️ Clearing MinIO data..."
	docker exec -it speedtest_c-minio-1 mc rm --recursive --force speedtest/warehouse/ || echo "No data to clear"
	@echo "✅ MinIO data cleared"

postgres-clear:
	@echo "🗑️ Clearing PostgreSQL data..."
	docker-compose down
	docker volume rm speedtest_c_pg-data || echo "Volume not found"
	docker-compose up -d postgres
	@echo "✅ PostgreSQL data cleared"

# Quick Setup Commands
setup:
	@echo "🚀 Quick setup - building and starting all services..."
	make build
	make up
	@echo "⏳ Waiting for services to start..."
	sleep 30
	make test

reset:
	@echo "🔄 Complete reset - cleaning and restarting..."
	make clean
	make setup

# Development Commands
dev-logs:
	@echo "📋 Following logs for development..."
	docker-compose logs -f spark airflow trino

spark-logs:
	@echo "⚡ Spark logs..."
	docker logs -f speedtest_c-spark-1

airflow-logs:
	@echo "🛠️ Airflow logs..."
	docker logs -f speedtest_c-airflow-1

# Health Check Commands
health:
	@echo "🏥 Health check..."
	@echo "Checking service status..."
	make status
	@echo ""
	@echo "Checking connectivity..."
	make test
	@echo ""
	@echo "Checking data flow..."
	docker exec -it speedtest_c-trino-1 trino --server localhost:8080 --catalog iceberg_catalog --schema bronze --execute "SELECT COUNT(*) as bronze_count FROM speedtest_raw;" 2>/dev/null || echo "Bronze table not ready"
	docker exec -it speedtest_c-trino-1 trino --server localhost:8080 --catalog iceberg_catalog --schema silver --execute "SELECT COUNT(*) as silver_count FROM speedtest_clean;" 2>/dev/null || echo "Silver table not ready"
	docker exec -it speedtest_c-trino-1 trino --server localhost:8080 --catalog iceberg_catalog --schema gold --execute "SELECT COUNT(*) as gold_count FROM speedtest_agg;" 2>/dev/null || echo "Gold table not ready"

# Documentation
docs:
	@echo "📚 Opening documentation..."
	@echo "README: Check README.md for detailed documentation"
	@echo "Grafana: http://localhost:3000"
	@echo "Prometheus: http://localhost:9090"
	@echo "Airflow: http://localhost:8082"
	@echo "Kafdrop: http://localhost:9002"
	@echo "MinIO: http://localhost:9001" 