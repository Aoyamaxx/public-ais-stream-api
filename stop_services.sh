#!/bin/bash

# Add execution permission to the current script
chmod +x "$(readlink -f "$0")"

# Set the working directory to the directory of the script
cd "$(dirname "$0")"

echo "========================================"
echo "Stopping AIS Collection Services (docker-compose.yml)"
echo "========================================"
echo "Stopping services:"
echo "  • AIS Collector (ais_collector)"
echo "  • L&W Correction Service (lw_correction_service)" 
echo "  • Cloud SQL Proxy (cloud_sql_proxy)"

docker compose -f docker-compose.yml down

echo "========================================"
echo "Stopping Data Processing Services (data_process/docker-compose-process.yml)"
echo "========================================"
echo "Stopping services:"
echo "  • Data Processor (ais_data_processor)"
echo "  • Cloud SQL Proxy Processor (cloud_sql_proxy_processor)"

cd data_process
docker compose -f docker-compose-process.yml down

echo "========================================"
echo "All Services Stopped Successfully"
echo "========================================"

# List any remaining containers
echo "Checking for any remaining containers:"
remaining_containers=$(docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Image}}")
if [ "$(docker ps -q)" ]; then
    echo "$remaining_containers"
else
    echo "✓ No containers are currently running"
fi

# Show cleanup information
echo ""
echo "========================================"
echo "Cleanup Information"
echo "========================================"
echo "Services have been stopped. If you want to:"
echo ""
echo "• Restart all services:     bash start_services.sh"
echo "• View service logs:        docker logs [container_name]"
echo "• Remove all containers:    docker container prune -f"
echo "• Remove all images:        docker image prune -a -f"
echo "• Remove all volumes:       docker volume prune -f"
echo ""
echo "Note: Containers are configured with 'restart: always'."
echo "They will automatically restart after system reboots or Docker service restarts." 