FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the metrics exporter
COPY table_metrics_exporter.py .

# Expose Prometheus metrics port
EXPOSE 8000

# Run the exporter
CMD ["python", "table_metrics_exporter.py"] 