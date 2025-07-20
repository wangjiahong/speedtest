#!/usr/bin/env python3
"""
Grafana Setup Script
Automatically configures Grafana with Prometheus data source and imports dashboard
"""

import requests
import json
import time
import os

# Grafana configuration
GRAFANA_URL = "http://localhost:3000"
GRAFANA_USER = "admin"
GRAFANA_PASSWORD = "admin"

def wait_for_grafana():
    """Wait for Grafana to be ready"""
    print("⏳ Waiting for Grafana to start...")
    for i in range(30):
        try:
            response = requests.get(f"{GRAFANA_URL}/api/health")
            if response.status_code == 200:
                print("✅ Grafana is ready!")
                return True
        except:
            pass
        time.sleep(2)
        print(f"   Attempt {i+1}/30...")
    return False

def setup_prometheus_datasource():
    """Add Prometheus as data source"""
    print("🔗 Setting up Prometheus data source...")
    
    # Login to get API key
    login_data = {
        "user": GRAFANA_USER,
        "password": GRAFANA_PASSWORD
    }
    
    try:
        response = requests.post(f"{GRAFANA_URL}/api/login", json=login_data)
        if response.status_code != 200:
            print("❌ Failed to login to Grafana")
            return False
        
        # Add Prometheus data source
        datasource_config = {
            "name": "Prometheus",
            "type": "prometheus",
            "url": "http://prometheus:9090",
            "access": "proxy",
            "isDefault": True
        }
        
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            f"{GRAFANA_URL}/api/datasources",
            json=datasource_config,
            headers=headers
        )
        
        if response.status_code == 200:
            print("✅ Prometheus data source added successfully!")
            return True
        else:
            print(f"❌ Failed to add data source: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error setting up data source: {e}")
        return False

def import_dashboard():
    """Import the table metrics dashboard"""
    print("📊 Importing dashboard...")
    
    # Dashboard JSON
    dashboard_json = {
        "dashboard": {
            "title": "Data Pipeline Metrics",
            "tags": ["speedtest", "data-pipeline"],
            "style": "dark",
            "timezone": "browser",
            "panels": [
                {
                    "id": 1,
                    "title": "Table Row Counts",
                    "type": "stat",
                    "targets": [
                        {
                            "expr": "bronze_table_row_count",
                            "legendFormat": "Bronze Table"
                        },
                        {
                            "expr": "silver_table_row_count",
                            "legendFormat": "Silver Table"
                        },
                        {
                            "expr": "gold_table_row_count",
                            "legendFormat": "Gold Table"
                        }
                    ],
                    "fieldConfig": {
                        "defaults": {
                            "color": {"mode": "palette-classic"},
                            "custom": {
                                "displayMode": "list",
                                "orientation": "horizontal"
                            }
                        }
                    },
                    "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0}
                },
                {
                    "id": 2,
                    "title": "Data Flow Timeline",
                    "type": "timeseries",
                    "targets": [
                        {
                            "expr": "bronze_table_row_count",
                            "legendFormat": "Bronze"
                        },
                        {
                            "expr": "silver_table_row_count",
                            "legendFormat": "Silver"
                        },
                        {
                            "expr": "gold_table_row_count",
                            "legendFormat": "Gold"
                        }
                    ],
                    "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0}
                }
            ],
            "time": {"from": "now-1h", "to": "now"},
            "refresh": "30s"
        }
    }
    
    try:
        # Login to get API key
        login_data = {"user": GRAFANA_USER, "password": GRAFANA_PASSWORD}
        response = requests.post(f"{GRAFANA_URL}/api/login", json=login_data)
        
        if response.status_code != 200:
            print("❌ Failed to login to Grafana")
            return False
        
        # Import dashboard
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            f"{GRAFANA_URL}/api/dashboards/db",
            json=dashboard_json,
            headers=headers
        )
        
        if response.status_code == 200:
            print("✅ Dashboard imported successfully!")
            return True
        else:
            print(f"❌ Failed to import dashboard: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error importing dashboard: {e}")
        return False

def main():
    """Main setup function"""
    print("🚀 Setting up Grafana for Data Pipeline Monitoring...")
    
    # Wait for Grafana
    if not wait_for_grafana():
        print("❌ Grafana is not responding. Please check if it's running.")
        return
    
    # Setup data source
    if not setup_prometheus_datasource():
        print("❌ Failed to setup Prometheus data source")
        return
    
    # Import dashboard
    if not import_dashboard():
        print("❌ Failed to import dashboard")
        return
    
    print("\n🎉 Setup complete!")
    print("📊 Open Grafana: http://localhost:3000")
    print("📈 Your dashboard should now be available!")
    print("🔍 If you don't see data, make sure:")
    print("   - Data generator is running: make data-generate")
    print("   - Spark jobs have been executed")
    print("   - Table metrics exporter is working")

if __name__ == "__main__":
    main() 