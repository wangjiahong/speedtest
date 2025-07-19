-- Create Iceberg database and user if not exists
CREATE DATABASE IF NOT EXISTS iceberg;
CREATE USER IF NOT EXISTS iceberg WITH PASSWORD 'iceberg';
GRANT ALL PRIVILEGES ON DATABASE iceberg TO iceberg;

-- Create Iceberg catalog tables
\c iceberg;

-- Create namespaces table
CREATE TABLE IF NOT EXISTS iceberg_namespaces (
    namespace_id BIGINT PRIMARY KEY,
    namespace_name VARCHAR(255) NOT NULL,
    namespace_properties TEXT
);

-- Create tables table
CREATE TABLE IF NOT EXISTS iceberg_tables (
    table_id BIGINT PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    namespace_id BIGINT REFERENCES iceberg_namespaces(namespace_id),
    table_location VARCHAR(1000),
    table_properties TEXT
);

-- Insert default namespaces
INSERT INTO iceberg_namespaces (namespace_id, namespace_name) VALUES 
(1, 'bronze'),
(2, 'silver'),
(3, 'gold')
ON CONFLICT (namespace_id) DO NOTHING; 