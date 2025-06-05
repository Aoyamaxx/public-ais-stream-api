# Quick Start Guide: AIS Data Collection System

## Prerequisites

Before starting the AIS Data Collection System, ensure the following requirements are met:

**System Requirements**
- Docker and Docker Compose installed on your system
- Minimum 4GB RAM and 10GB available disk space
- Network connectivity for WebSocket and database connections

**Required Credentials**
- Google Cloud service account JSON file with Cloud SQL Client permissions
- AISStream.io API key for real-time AIS data access
- Access to a Google Cloud SQL PostgreSQL instance

## Installation Steps

### Step 1: Repository Setup

Clone the repository and navigate to the project directory:

```bash
git clone https://github.com/NorthSeaWatch/ais-stream-api.git
cd ais-stream-api
```

### Step 2: Service Account Configuration

Place your Google Cloud service account JSON file in the project root directory. The file should be named `north-sea-watch-d8ad3753e506.json` or update the `docker-compose.yml` file to reflect your filename.

### Step 3: Environment Configuration

Update the environment variables in `docker-compose.yml`:

```yaml
environment:
  DB_NAME: "your_database_name"
  DB_USER: "your_database_user"
  DB_PASSWORD: "your_database_password"
  DB_HOST: "cloud_sql_proxy"
  DB_PORT: "5432"
  API_KEY: "your_aisstream_api_key"
```

Update the Cloud SQL instance connection string in the `cloud_sql_proxy` service command:

```yaml
command: >
  /cloud_sql_proxy
  -instances=your-project:your-region:your-instance=tcp:0.0.0.0:5432
  -credential_file=/secrets/service_account.json
```

### Step 4: System Startup

Start the complete system using the management interface:

```bash
python manage_services.py start
```

This command will build Docker images, start all services, and verify connectivity.

## Verification and Monitoring

### Check Service Status

Verify all services are running correctly:

```bash
python manage_services.py status
```

Expected output should show three running containers:
- `cloud_sql_proxy` - Database connection management
- `ais_collector` - Real-time data collection
- `lw_correction_service` - Data quality monitoring

### Monitor System Logs

View real-time logs for system monitoring:

```bash
# All services
python manage_services.py logs --follow

# Specific service
python manage_services.py logs --service ais_collector --follow
```

### Database Connectivity Verification

Connect to your Cloud SQL instance to verify data collection:

```sql
-- Check ship count
SELECT COUNT(*) FROM ships;

-- Check recent data
SELECT * FROM ship_data ORDER BY timestamp_collected DESC LIMIT 10;

-- Check data collection status
SELECT COUNT(*) as total_records, 
       MAX(timestamp_collected) as latest_record 
FROM ship_data;
```

## Data Processing and Analysis

### Generate Processed Dataset

Create the main analysis dataset:

```bash
python data_process/process_ais_data.py
```

This generates `processed_ais_data_YYYYMMDD_YYYYMMDD_sampled_100.parquet` in the `data/` directory.

### Create Visualizations

Generate interactive visualizations:

```bash
python data_process/visualize_scrubber_data.py
```

Output files in the `data/` directory:
- `scrubber_heatmap.html` - Spatial distribution heatmap
- `scrubber_points_map.html` - Individual vessel positions
- `scrubber_sankey.html` - Ship type to destination flows
- `ship_counts_time_series.html` - Temporal analysis

### Run Pollution Analysis

Execute pollution monitoring and forecasting:

```bash
# Current pollution analysis
python analyze_pollution.py

# Pollution forecasting
python forecast_pollution.py
```

## Common Operations

### System Management

**Stop the system:**
```bash
python manage_services.py stop
```

**Restart the system:**
```bash
python manage_services.py restart
```

**Clean system resources:**
```bash
python manage_services.py clean
```

### Data Processing Operations

**Process historical data:**
```bash
python data_process/data_process.py
```

**Import port data:**
```bash
python data_process/import_ports_to_db.py
```

**Augment AIS data:**
```bash
python augment_ais_data.py
```

## Troubleshooting

### Common Issues

**Service startup failures:**
- Verify Docker and Docker Compose installation
- Check service account JSON file location and permissions
- Validate environment variables in docker-compose.yml

**Database connection issues:**
- Verify Cloud SQL instance connection string
- Check service account permissions for Cloud SQL Client role
- Ensure Cloud SQL Admin API is enabled in your GCP project

**Data collection problems:**
- Verify AISStream.io API key validity
- Check network connectivity and firewall settings
- Monitor logs for WebSocket connection status

### System Monitoring

**Resource monitoring:**
```bash
docker stats
docker-compose ps
```

**Log analysis:**
```bash
docker-compose logs cloud_sql_proxy
docker-compose logs ais_collector
docker-compose logs lw_correction_service
```

### Performance Optimization

**Memory optimization:**
- Adjust container memory limits in docker-compose.yml
- Monitor database connection pool usage
- Optimize batch processing sizes for your system

**Storage management:**
- Implement log rotation for long-running deployments
- Monitor disk usage for data directory
- Consider data archival strategies for historical data

## Next Steps

### Data Analysis

Explore the generated datasets:
- Load `processed_ais_data_20250419_20250519_sampled_100.parquet` for analysis
- Use interactive visualizations for maritime research
- Implement custom analysis using the comprehensive dataset

### System Extension

Consider advanced configurations:
- Multiple geographic regions for expanded coverage
- Custom analysis scripts for specific research requirements
- Integration with external maritime databases
- Performance optimization for large-scale deployments

### Research Applications

Utilize the system for maritime research:
- Scrubber technology adoption studies
- Maritime traffic pattern analysis
- Environmental compliance monitoring
- Regional maritime commerce analysis

## Support and Documentation

**Comprehensive Documentation:**
- `README.md` - System overview and architecture
- `TECHNICAL_DOCUMENTATION.md` - Detailed technical specifications
- `DATA_ANALYSIS_GUIDE.md` - Dataset usage and analysis guidance
- `SERVICE_MANAGEMENT_GUIDE.md` - Operational procedures and maintenance

**Code Examples:**
- `analysis.ipynb` - Jupyter notebook with analysis examples
- Data processing scripts in `data_process/` directory
- Visualization examples in generated HTML files

For additional support, refer to the comprehensive documentation files and examine the codebase for implementation details and customization options. 