#!/bin/sh
# Wait for MinIO to be ready
sleep 5
# Configure mc (MinIO client)
mc alias set local http://minio:9000 minioadmin minioadmin
# Create bucket if not exists
mc mb --ignore-existing local/speedtest 