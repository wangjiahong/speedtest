# 🚀 SpeedTest Data Pipeline - Medallion Architecture

A comprehensive data engineering project demonstrating a **medallion architecture** for processing high-volume network performance data using open-source tools. This project showcases expertise in designing scalable data systems for **2TB/day** of structured data.

## 🏗️ Architecture Overview

### **Medallion Architecture Layers:**
- **🥉 Bronze Layer**: Raw data ingestion from Kafka
- **🥈 Silver Layer**: Cleaned and deduplicated data
- **🥇 Gold Layer**: Aggregated business metrics

### **Technology Stack:**
- **Apache Kafka** - Real-time data streaming
- **Apache Spark** - Data processing and transformation
- **Apache Iceberg** - Table format and ACID transactions
- **Trino** - Interactive SQL queries
- **Apache Airflow** - Workflow orchestration
- **MinIO** - S3-compatible object storage
- **PostgreSQL** - Iceberg catalog and metadata
- **Docker Compose** - Local development environment

## 📊 Data Flow

```
Data Generator → Kafka → Spark (Bronze) → Spark (Silver) → Spark (Gold) → Trino
     ↓              ↓         ↓              ↓              ↓           ↓
  JSON Data    Real-time   Raw Data    Clean Data   Aggregated   Business
  Generation   Streaming   Storage     Storage      Metrics      Queries
```

## 🚀 Quick Start

### **Prerequisites:**
- Docker and Docker Compose
- Git
- 8GB+ RAM available

### **1. Clone the Repository:**
```bash
git clone <your-repo-url>
cd speedtest_c
```

### **2. Start the Infrastructure:**
```bash
docker-compose up -d
```

### **3. Monitor Services:**
- **Airflow UI**: http://localhost:8082 (admin/admin)
- **Trino UI**: http://localhost:8083
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **Kafdrop**: http://localhost:9002
- **Grafana**: http://localhost:3000 (admin/admin)

### **4. Trigger Data Pipeline:**
1. Go to Airflow UI: http://localhost:8082
2. Enable the `medallion_pipeline` DAG
3. Trigger the DAG manually or wait for scheduled runs

## 📁 Project Structure

```
speedtest_c/
├── docker-compose.yml          # Infrastructure orchestration
├── data_gen/                   # Data generation
│   ├── generate_speedtest_data.py
│   ├── Dockerfile
│   └── requirements.txt
├── spark_jobs/                 # Spark processing jobs
│   ├── ingest_bronze.py        # Bronze layer ingestion
│   ├── bronze_to_silver.py     # Silver layer transformation
│   ├── silver_to_gold.py       # Gold layer aggregation
│   ├── iceberg_maintenance.py  # Table maintenance
│   └── register_tables.py      # Catalog registration
├── airflow/                    # Airflow DAGs
│   └── dags/
│       └── medallion_pipeline.py
├── trino/                      # Trino configuration
│   ├── catalog/
│   ├── config.properties
│   └── jvm.config
├── postgres/                   # PostgreSQL setup
│   └── init.sql
└── monitoring/                 # Prometheus & Grafana
    ├── prometheus.yml
    └── grafana/
```

## 🔧 Configuration

### **Environment Variables:**
- `KAFKA_BROKER`: Kafka bootstrap servers
- `KAFKA_TOPIC`: Topic name for speedtest data
- `RECORDS_TO_GENERATE`: Number of records to generate
- `MESSAGES_PER_SECOND`: Data generation rate

### **Data Schema:**
```json
{
  "timestamp": 1721414400000,
  "user_id": "550e8400-e29b-41d4-a716-446655440000",
  "upload_speed": 45.23,
  "download_speed": 120.45,
  "latency": 25.67,
  "location": "New York"
}
```

## 📈 Data Processing Pipeline

### **Bronze Layer (Raw Data):**
- Ingests JSON data from Kafka
- Stores raw data in Iceberg format
- Partitions by date for efficient querying

### **Silver Layer (Clean Data):**
- Removes null values and duplicates
- Validates data quality
- Maintains data lineage

### **Gold Layer (Business Metrics):**
- Hourly aggregations by location
- Average upload/download speeds
- Performance metrics and KPIs

## 🔍 Querying Data

### **Using Trino:**
```sql
-- Query bronze layer
SELECT * FROM iceberg_catalog.bronze.speedtest_raw LIMIT 10;

-- Query silver layer
SELECT * FROM iceberg_catalog.silver.speedtest_clean LIMIT 10;

-- Query gold layer (aggregated metrics)
SELECT 
    date,
    location,
    avg_upload_speed,
    avg_download_speed,
    avg_latency
FROM iceberg_catalog.gold.speedtest_agg
WHERE date = CURRENT_DATE;
```

### **Using DBeaver:**
1. Connect to Trino: `localhost:8083`
2. Use catalog: `iceberg_catalog`
3. Browse schemas: `bronze`, `silver`, `gold`

## 🛠️ Development

### **Adding New Spark Jobs:**
1. Create new Python file in `spark_jobs/`
2. Add to Airflow DAG in `airflow/dags/`
3. Test with `docker exec` commands

### **Modifying Data Schema:**
1. Update `data_gen/generate_speedtest_data.py`
2. Update Spark job schemas
3. Rebuild containers: `docker-compose build`

### **Scaling the Pipeline:**
- Increase Spark worker instances
- Add more Kafka partitions
- Scale MinIO storage
- Optimize Iceberg table properties

## 📊 Monitoring & Observability

### **Metrics Available:**
- Kafka message throughput
- Spark job execution times
- Trino query performance
- Storage usage metrics

### **Dashboards:**
- Real-time data flow monitoring
- Performance metrics visualization
- Error rate tracking
- Resource utilization

## 🚨 Troubleshooting

### **Common Issues:**

**1. Spark Job Failures:**
```bash
# Check Spark logs
docker logs speedtest_c-spark-1

# Verify Kafka connectivity
docker exec -it speedtest_c-kafka-1 kafka-topics --list
```

**2. Data Not Appearing:**
```bash
# Check data generator
docker logs speedtest_c-data-gen-1

# Verify Kafka messages
docker exec -it speedtest_c-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic speedtest_raw --from-beginning --max-messages 5
```

**3. Trino Connection Issues:**
```bash
# Check Trino logs
docker logs speedtest_c-trino-1

# Verify catalog configuration
docker exec -it speedtest_c-trino-1 cat /etc/trino/catalog/iceberg.properties
```

## 📚 Learning Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Trino Documentation](https://trino.io/docs/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🎯 Use Cases

This project demonstrates:
- **Real-time data processing** at scale
- **Data quality management** and validation
- **ACID transactions** with Iceberg
- **Interactive analytics** with Trino
- **Workflow orchestration** with Airflow
- **Monitoring and observability** best practices

Perfect for **Data Architect** positions requiring expertise in modern data stack technologies! 