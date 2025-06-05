# AIS Data Collection and Analysis System

This repository contains a comprehensive AIS (Automatic Identification System) data collection and analysis pipeline for monitoring ship activities in the North Sea region. The system collects real-time AIS data from ships, stores it in Google Cloud SQL, and provides extensive analysis capabilities focusing on scrubber-equipped vessels and pollution monitoring.

## System Architecture

The system consists of three main components:

### 1. Data Collection Services

**AIS Collector Service** (`ais_collector.py`) - Real-time data collection from AISStream.io WebSocket API with geographic filtering using North Sea shapefiles. The collector stores ship static data and position reports in separate database tables for ships with valid IMO numbers, while handling vessels without IMO separately.

**Length-Width Correction Service** (`lw_correction_service.py`) - Provides automated data quality improvements by correcting ship dimension inconsistencies and maintaining data integrity across the database.

### 2. Data Processing Pipeline

**Primary Data Processor** (`data_process/process_ais_data.py`) - Processes raw AIS data in configurable chunks to avoid memory constraints. The processor joins AIS data with ICCT/WFR scrubber databases to identify scrubber-equipped vessels and generates standardized datasets for analysis.

**Advanced Data Processor** (`data_process/data_process.py`) - Handles comprehensive data normalization including ship type code mapping, port information integration, and navigational status standardization. Supports both incremental updates and full database recreation modes.

### 3. Analysis and Visualization

**Scrubber Analysis Visualizer** (`data_process/visualize_scrubber_data.py`) - Creates interactive visualizations including heatmaps showing spatial distribution of scrubber-equipped ships, Sankey diagrams for ship type and destination flows, and temporal analysis charts.

**Pollution Analysis Engine** (`analyze_pollution.py`) - Implements regulatory compliance checking for scrubber discharge based on international maritime laws, port restrictions, and country-specific regulations. Calculates emission estimates considering different operational modes.

**Pollution Forecasting System** (`forecast_pollution.py`) - Uses machine learning models to predict future pollution levels by country using time series analysis with lag features and confidence intervals.

## Database Schema

### Core Tables

**ships** - Static vessel information for ships with valid IMO numbers including vessel specifications and identifiers.

**ship_data** - Dynamic position and operational data linked to valid IMO ships with timestamps, coordinates, and navigational parameters.

**unknown_ships** - Combined static and dynamic information for vessels without valid IMO numbers, maintaining data completeness.

### Reference Tables

**ship_type_codes** - Standardized ship type classifications with descriptions and remarks.

**ports** - Port coordinates, country assignments, and scrubber discharge regulations.

**navigational_status** - Standardized navigational status code mappings.

**icct_wfr_combined** - Integrated database of ships equipped with scrubber systems from ICCT and WFR sources.

## Data Analysis Framework

### Primary Dataset

The analysis framework utilizes `processed_ais_data_20250419_20250519_sampled_100.parquet` as the main data source for research and visualization. This dataset contains processed AIS records with scrubber identification, ship classifications, and temporal sampling for computational efficiency.

### Analytical Capabilities

**Scrubber Ship Analysis** - Comprehensive identification and tracking of vessels equipped with scrubber systems using cross-referenced maritime databases. Analysis includes spatial distribution patterns, operational behavior differences, and destination preferences.

**Pollution Monitoring** - Real-time assessment of scrubber discharge compliance based on vessel location, operational mode, and applicable maritime regulations. The system implements complex rule sets for different jurisdictions and port authorities.

**Temporal Pattern Analysis** - Time series analysis of ship movements, seasonal variations, and operational pattern changes. The system provides forecasting capabilities for pollution levels and vessel traffic patterns.

**Geospatial Analysis** - Advanced spatial analysis using precise North Sea boundary definitions with shapefile-based geographic filtering. The system supports complex queries for vessel density, route analysis, and regional activity monitoring.

## Deployment Configuration

### Docker Infrastructure

The system deploys using Docker Compose with three coordinated services:

- **cloud_sql_proxy** - Secure connection management to Google Cloud SQL instance
- **ais_collector** - Real-time AIS data collection and database insertion
- **lw_correction_service** - Continuous data quality monitoring and correction

### Service Management

**Automated Management** (`manage_services.py`) - Comprehensive service lifecycle management with commands for starting, stopping, monitoring, and troubleshooting all system components.

**Manual Control Scripts** - Direct shell scripts (`start_services.sh`, `stop_services.sh`) for system administration without Python dependencies.

### Environment Configuration

The system requires configuration of database credentials, AIS API keys, and Cloud SQL connection parameters through environment variables or Docker Compose configuration.

## Requirements and Dependencies

**Core Dependencies** - asyncio, websockets, psycopg2-binary, sqlalchemy, pandas, geopandas, shapely for data collection and processing.

**Analysis Libraries** - plotly, folium, numpy, matplotlib, seaborn for visualization and statistical analysis.

**Cloud Integration** - cloud-sql-python-connector, pyarrow for efficient data storage and cloud database connectivity.

**Machine Learning** - scikit-learn components integrated into pollution forecasting algorithms.

## Installation and Setup

Clone the repository and navigate to the project directory. Ensure Docker and Docker Compose are installed on the target system. Place the Google Cloud service account JSON file in the project root directory.

Configure environment variables in `docker-compose.yml` including database credentials, Cloud SQL instance connection string, and AIS API key. Verify the Cloud SQL instance connection name matches your Google Cloud project configuration.

Start the system using the management interface:

```bash
python manage_services.py start
```

Monitor system status and logs:

```bash
python manage_services.py status
python manage_services.py logs --follow
```

## Data Processing Workflows

### Real-time Collection

The AIS collector continuously receives vessel position reports and static data through WebSocket connections. Data undergoes immediate geographic filtering using North Sea boundary shapefiles before database insertion.

### Batch Processing

Historical data processing occurs in configurable time chunks to manage memory usage efficiently. The processor handles large datasets by implementing sampling strategies and incremental processing approaches.

### Quality Assurance

The length-width correction service monitors data quality continuously, identifying and correcting dimensional inconsistencies in vessel specifications. The system maintains audit trails for all data modifications.

## Visualization Outputs

**Interactive Maps** - HTML-based visualizations including radiation-style heatmaps for scrubber ship density, point maps with vessel details, and temporal animation capabilities.

**Flow Diagrams** - Sankey visualizations showing relationships between ship types, scrubber status, and destination ports with quantitative flow analysis.

**Time Series Charts** - Interactive plots comparing scrubber versus non-scrubber vessel activities over time with trend analysis and forecasting.

**Pollution Reports** - Comprehensive country-level pollution analysis with regulatory compliance assessment and forecasting models.

## Data Export and Integration

The system generates multiple data formats for analysis including Parquet files for efficient data storage, CSV exports for external integration, and HTML visualizations for interactive exploration.

Analysis results support integration with external research platforms and regulatory reporting systems through standardized data formats and comprehensive metadata documentation.