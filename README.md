# AIS Data Collector with Cloud SQL

This project collects AIS (Automatic Identification System) data from ships in the North Sea and stores the data in Google Cloud SQL (PostgreSQL). It is designed to run in a Docker environment on a remote server or VM. Logs are accessible via Docker logs, and it is possible to query the AIS data directly in Cloud SQL for further analysis.

## Table of Contents
- [Project Overview](#project-overview)
- [Requirements](#requirements)
- [Deployment to a Remote Server](#deployment-to-a-remote-server)
- [Data Collection Process](#data-collection-process)
- [Connecting to Cloud SQL](#connecting-to-cloud-sql)
- [Stopping or Restarting the Collector](#stopping-or-restarting-the-collector)

## 1. Project Overview
- **Data Source**: AISStream.io WebSocket API (real-time AIS data).
- **Collected Data**: Ship static information and movement data in the North Sea.
- **Storage**: Google Cloud SQL for PostgreSQL.
- **Deployment**: Docker (using Docker Compose) + Cloud SQL Auth Proxy + Any Cloud VM.
- **Primary Tables**:
  - `ships` / `ship_data` (for IMO > 0)
  - `unknown_ships` (for ships with no valid IMO number)
- **Cloud SQL Connection**: Refer to the example in `analysis.ipynb`, which demonstrates how to use the Cloud SQL Auth Proxy to connect to the database for further analysis.

## 2. Requirements
- Docker and Docker Compose on remote server or VM.
- Google Cloud SQL instance (PostgreSQL).
- Cloud SQL Auth Proxy credentials:
  - A Service Account JSON file with Cloud SQL Client permission.
  - The Cloud SQL Admin API enabled in GCP project.
- AIS API key from AISStream.io.

Verify Docker and Docker Compose installation:

```sh
sudo docker --version
```
```sh
sudo docker compose version
```

## 3. Deployment to a Remote Server

### Clone the Repository

```sh
git clone https://github.com/NorthSeaWatch/ais-stream-api.git
```
```sh
cd ais-stream-api
```

### Add Service Account JSON

- Generate and download Service Account key JSON file (e.g. `north-sea-watch-39a510f80808.json`) into the project root.
- In the `docker-compose.yml`, update the volumes section so it mounts this JSON file inside the `cloud_sql_proxy` container.

### Edit Docker Compose Environment Variables

In `docker-compose.yml` (or via environment variables), set the following:

```yaml
environment:
  DB_NAME: "DB_NAME"
  DB_USER: "DB_USER"
  DB_PASSWORD: "DB_PASSWORD"
  DB_HOST: "cloud_sql_proxy"
  DB_PORT: "5432"
  API_KEY: "AIS_API_KEY"
```

Make sure `-instances=<PROJECT_ID>:<REGION>:<INSTANCE_NAME>=tcp:0.0.0.0:5432` in `cloud_sql_proxy` command matches Cloud SQL instance connection name.

### Start the Collector

```sh
sudo docker compose up -d --build
```

- `-d` runs in detached mode.

Check running containers:

```sh
sudo docker compose ps
```

## 4. Data Collection Process

Once the Docker containers are running, the Python script:

- Connects to Cloud SQL instance through the Auth Proxy.
- Creates or updates tables: `ships`, `ship_data`, and `unknown_ships`.
- Connects to the AIS WebSocket API for the bounding box `[[50.0, -5.0], [61.0, 10.0]]`.
- Continuously inserts or updates records based on IMO or stores unknown IMO data in `unknown_ships`.

Logs can be view with:

```sh
sudo docker logs -f ais_python
```

OR

```sh
sudo docker compose logs -f python_app
```

## 5. Connecting to Cloud SQL

### Cloud SQL Query Editor

- If Google Cloud Console is directly accessible, navigate to **SQL → Instance → Query Editor**.
- Enter database username/password.
- Run queries, for example:

```sql
SELECT COUNT(*) FROM ships;
SELECT * FROM ship_data ORDER BY timestamp_ais DESC LIMIT 10;
SELECT COUNT(*) FROM unknown_ships WHERE imo_number = -1;
```

### psql Command Line

- Enable a public IP for instance and add client IP to the “Authorized networks” in Cloud SQL, or
- Use the Cloud SQL Auth Proxy locally.

Then connect:

```sh
psql --host=<YOUR_CLOUDSQL_IP/localhost> --port=5432 \
     --dbname=YOUR_DB_NAME \
     --username=YOUR_DB_USER
```

## 6. Stopping or Restarting the Collector

### Stop all Docker containers:

```sh
sudo docker compose down
```

This halts the AIS collector and the Cloud SQL Proxy. Data remains in Cloud SQL instance.

### Restart with:

```sh
sudo docker compose up -d
```

The collector will resume inserting data.

## 7. Data Processing and Visualization

The repository includes scripts for processing the collected AIS data and creating visualizations for analysis, particularly focusing on ships with scrubber systems.

### Processing AIS Data

The `process_ais_data.py` script processes raw AIS data from the database, creating aggregated datasets for analysis:

```sh
python data_process/process_ais_data.py
```

This script:
- Connects to the database and extracts AIS data within a specified date range
- Identifies ships with scrubbers by matching against ICCT/WFR databases
- Processes data in chunks to avoid memory issues
- Saves processed data to parquet files in the `data/` directory
- Performs preliminary analyses on ship movements and generates basic statistics

### Generating Visualizations

After processing the data, run the visualization script to generate interactive graphs and maps:

```sh
python data_process/visualize_scrubber_data.py
```

This script generates several visualizations:

1. **Scrubber Ship Heatmap** (`data/scrubber_heatmap.html`): A radiation-style heatmap showing the spatial distribution and density of scrubber-equipped ships in the North Sea.

2. **Ship Position Map** (`data/scrubber_points_map.html`): A map showing individual scrubber ship positions with popup information.

3. **Sankey Diagrams** (`data/scrubber_sankey.html` and `data/non_scrubber_sankey.html`): Flow diagrams showing the relationships between ship types and destinations.

4. **Time Series Plot** (`data/ship_counts_time_series.html`): Trends of scrubber vs. non-scrubber ships over time.

### Viewing the Visualizations

The visualizations are saved as HTML files and can be opened in any web browser:

```sh
open data/scrubber_heatmap.html  # On macOS
# or use your browser to open the HTML files directly
```

The interactive visualizations allow:
- Zooming and panning on maps
- Toggling different layers
- Viewing tooltips with additional information
- Switching between map styles
