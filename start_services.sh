#!/bin/bash

# Add execution permission to the current script
chmod +x "$(readlink -f "$0")"

# Set working directory to the script's directory
cd "$(dirname "$0")"

echo "========================================"
echo "Starting AIS Collection Services (docker-compose.yml)"
echo "========================================"
# First start the cloud_sql_proxy service
docker compose -f docker-compose.yml up -d --build cloud_sql_proxy

# Wait for the cloud_sql_proxy service to start
echo "Waiting for cloud_sql_proxy service to start..."
sleep 10  # Give cloud_sql_proxy some startup time

# Check if the cloud_sql_proxy is healthy
retry_count=0
max_retries=5
while ! docker logs cloud_sql_proxy 2>&1 | grep -q "Ready for new connections"; do
  retry_count=$((retry_count+1))
  if [ $retry_count -ge $max_retries ]; then
    echo "cloud_sql_proxy service did not start properly, please check the logs"
    docker logs cloud_sql_proxy
    exit 1
  fi
  echo "cloud_sql_proxy service is not ready, waiting 5 seconds..."
  sleep 5
done

echo "cloud_sql_proxy service is ready, starting AIS services..."

# Start the AIS Collector
echo "Starting AIS Collector..."
docker compose -f docker-compose.yml up -d --build ais_collector

# Start the L&W Correction Service
echo "Starting L&W Correction Service..."
docker compose -f docker-compose.yml up -d --build lw_correction_service

echo "========================================"
echo "Starting Data Processing Services (data_process/docker-compose-process.yml)"
echo "========================================"
cd data_process
# First start the cloud_sql_proxy_processor service
docker compose -f docker-compose-process.yml up -d --build cloud_sql_proxy

# Wait for the cloud_sql_proxy_processor service to start
echo "Waiting for the cloud_sql_proxy_processor service to start..."
sleep 10

# Check if the cloud_sql_proxy_processor is healthy
retry_count=0
max_retries=5
while ! docker logs cloud_sql_proxy_processor 2>&1 | grep -q "Ready for new connections"; do
  retry_count=$((retry_count+1))
  if [ $retry_count -ge $max_retries ]; then
    echo "cloud_sql_proxy_processor service did not start properly, please check the logs"
    docker logs cloud_sql_proxy_processor
    exit 1
  fi
  echo "cloud_sql_proxy_processor service is not ready, waiting 5 seconds..."
  sleep 5
done

echo "cloud_sql_proxy_processor service is ready, starting data processor..."

# Start the dependent services
docker compose -f docker-compose-process.yml up -d --build data_processor

echo "========================================"
echo "All Services Started Successfully"
echo "========================================"
echo "Services running:"
echo "  • AIS Collector (ais_collector.py)"
echo "  • L&W Correction Service (lw_correction_service.py)"
echo "  • Data Processor"
echo "  • Cloud SQL Proxies"

# Display running containers
echo ""
echo "Running containers:"
docker ps 

echo ""
echo "To view logs:"
echo "  AIS Collector:        docker logs ais_collector"
echo "  L&W Correction:       docker logs lw_correction_service" 
echo "  Data Processor:       docker logs ais_data_processor"
