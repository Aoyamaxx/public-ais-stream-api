# Technical Documentation: AIS Data Collection and Analysis System

## System Architecture Overview

The AIS Data Collection and Analysis System implements a multi-tier architecture designed for scalable real-time data processing and comprehensive maritime analysis. The system processes approximately 50,000-100,000 AIS messages daily from the North Sea region.

## Core Services Architecture

### AIS Collector Service (`ais_collector.py`)

**Connection Management**
- Maintains persistent WebSocket connection to AISStream.io API
- Implements automatic reconnection with exponential backoff
- Processes messages: `ShipStaticData` and `PositionReport`
- Geographic filtering using bounding box `[[50.0, -5.0], [61.5, 13.0]]`

**Data Processing Pipeline**
- Primary geographic filter using bounding box coordinates
- Secondary geometric filtering using North Sea shapefile (`north_sea_watch_region_patched.shp`)
- Batch processing with configurable `BATCH_SIZE = 100`
- Database connection pooling with retry mechanisms (`MAX_DB_RETRIES = 3`)

**Database Integration**
- PostgreSQL connection through Cloud SQL Auth Proxy
- Separate table management for valid IMO vessels (`ships`, `ship_data`) and unknown vessels (`unknown_ships`)
- Transaction-based batch insertions for data consistency
- MMSI-to-IMO mapping cache for performance optimization

### Length-Width Correction Service (`lw_correction_service.py`)

**Data Quality Framework**
- Continuous monitoring of vessel dimension inconsistencies
- Statistical analysis for outlier detection in length and width measurements
- Automated correction using historical data patterns
- Audit logging for all data modifications

**Correction Algorithms**
- Cross-referencing multiple AIS message sources for dimension validation
- Time-series analysis for detecting measurement errors
- Machine learning-based anomaly detection for dimension outliers
- Manual override capabilities for confirmed specifications

### Cloud SQL Proxy Integration

**Security Architecture**
- Service account authentication using JSON key files
- Encrypted connection tunneling to Cloud SQL instance
- Connection string: `north-sea-watch:europe-west4:ais-database`
- Port mapping: `tcp:0.0.0.0:5432` for internal container communication

## Data Processing Pipeline

### Primary Data Processor (`data_process/process_ais_data.py`)

**Memory Management**
- Chunk-based processing with configurable `chunk_size_days = 7`
- Sampling strategies with configurable intervals (`sample_interval = 1000`)
- Pandas DataFrame optimization using `pyarrow` backend
- Garbage collection management for large dataset processing

**Scrubber Identification**
- Cross-referencing with ICCT (International Council on Clean Transportation) database
- WFR (World Fleet Register) integration for scrubber-equipped vessels
- Boolean flag assignment (`has_scrubber`) for analytical queries
- Performance optimization using indexed database joins

**Statistical Analysis**
- Ship movement pattern analysis using temporal and spatial clustering
- Average speed calculations per vessel and route
- Destination frequency analysis and route optimization studies
- Unique destination counting for vessel behavior profiling

### Advanced Data Processor (`data_process/data_process.py`)

**Database Migration System**
- Schema versioning and automatic migration capabilities
- Column addition and modification without data loss
- Reference table management (`ship_type_codes`, `ports`, `navigational_status`)
- Rollback capabilities for failed migrations

**Parallel Processing**
- Multi-threaded processing for ships and ship_data tables
- Connection pooling using `psycopg2.pool.SimpleConnectionPool`
- Batch processing with configurable sizes (`batch_size = 10000`)
- Thread-safe database operations with transaction management

**Data Normalization**
- Ship type code standardization using IMO classifications
- Port name normalization and country assignment
- Navigational status code mapping to human-readable descriptions
- Temporal data standardization and timezone handling

## Analysis and Visualization Framework

### Scrubber Analysis Visualizer (`data_process/visualize_scrubber_data.py`)

**Interactive Mapping**
- Folium-based map generation with multi-layer support
- HeatMap plugin for density visualization using radiation-style rendering
- Custom color schemes for scrubber and non-scrubber vessels
- Zoom-level optimization for different analysis scales

**Sankey Diagram Generation**
- Plotly-based flow visualization for ship type to destination relationships
- Top-N filtering for clarity (`top_n = 5` configurable)
- Color-coded ship type and destination categories
- Quantitative flow analysis with unique vessel counting

**Destination Normalization**
- Regular expression-based port name standardization
- Multi-language port name mapping and correction
- Combined destination splitting for vessels with multiple stops
- Secondary destination tracking for comprehensive route analysis

### Pollution Analysis Engine (`analyze_pollution.py`)

**Regulatory Compliance Framework**
- Port-specific discharge restrictions implementation
- Country-level maritime law compliance checking
- Distance-based restriction enforcement (e.g., 3 nautical miles)
- Inland waterway detection using land boundary datasets

**Emission Calculation**
- Operational mode-based emission factors (`emission_berth`, `emission_anchor`, `emission_maneuver`, `emission_cruise`)
- Time-weighted emission calculations using position report intervals
- Regulatory compliance filtering before emission counting
- Country-level aggregation with daily breakdown capabilities

**Geospatial Analysis**
- Natural Earth dataset integration for land boundary detection
- Haversine formula implementation for accurate distance calculations
- Point-in-polygon analysis for territorial water determination
- Shapely geometry operations for complex geographical queries

### Pollution Forecasting System (`forecast_pollution.py`)

**Machine Learning Pipeline**
- Random Forest Regressor with 100 estimators for robust predictions
- Feature engineering including lag features (1, 7, 14 days)
- Temporal features (day of week, month, day) for seasonal patterns
- Cross-validation and confidence interval estimation

**Time Series Analysis**
- Autoregressive features for trend continuation
- Seasonal decomposition for periodic pattern identification
- Confidence interval calculation using ensemble variance
- Multi-step ahead forecasting with recursive prediction

## Database Schema and Optimization

### Core Table Design

**ships Table**
- Primary key: `imo_number` (BIGINT) for international vessel identification
- Foreign key relationships with `ship_data` for referential integrity
- Indexes on `mmsi` and `name` for query optimization
- Vessel specification fields with appropriate data types and constraints

**ship_data Table**
- Auto-incrementing primary key for unique position records
- Foreign key reference to `ships.imo_number` for data consistency
- Compound indexes on (`imo_number`, `timestamp_collected`) for temporal queries
- Spatial indexes on (`latitude`, `longitude`) for geographic queries

**unknown_ships Table**
- Consolidated static and dynamic data for vessels without valid IMO
- Separate indexing strategy for MMSI-based queries
- Temporal partitioning considerations for large dataset management

### Performance Optimization

**Indexing Strategy**
- B-tree indexes on frequently queried columns
- Composite indexes for multi-column WHERE clauses
- Partial indexes for filtered queries (e.g., valid coordinates only)
- Regular index maintenance and statistics updates

**Query Optimization**
- Prepared statement usage for repeated queries
- Connection pooling to reduce connection overhead
- Batch operations for bulk data insertions
- Query plan analysis and optimization for complex analytical queries

## Deployment and Infrastructure

### Docker Container Architecture

**Base Image Configuration**
- Python 3.9+ runtime with optimized package installations
- Minimal security surface with non-root user execution
- Multi-stage builds for production image size optimization
- Health checks for container orchestration

**Network Configuration**
- Internal Docker network for service communication
- Port exposure only for necessary external connections
- Service discovery using Docker Compose service names
- Load balancing considerations for high availability

### Production Deployment Considerations

**Scalability Planning**
- Horizontal scaling capabilities for increased data volume
- Database connection pool sizing for concurrent operations
- Memory allocation optimization for large dataset processing
- CPU resource allocation for computational analysis tasks

**Monitoring and Alerting**
- Container health monitoring using Docker health checks
- Database connection monitoring and automatic recovery
- Data quality metrics tracking and alerting
- Performance metrics collection for system optimization

**Data Backup and Recovery**
- Automated Cloud SQL backup scheduling
- Point-in-time recovery capabilities for data protection
- Export procedures for data archival and analysis
- Disaster recovery procedures and testing protocols

## Security Framework

### Authentication and Authorization

**Service Account Management**
- Principle of least privilege for Cloud SQL access
- JSON key file protection and rotation procedures
- IAM role assignments for specific database operations
- Regular security audit and access review processes

**Data Protection**
- Encryption in transit using TLS for all database connections
- Encryption at rest provided by Cloud SQL managed service
- API key protection for AISStream.io service access
- Secure environment variable management

### Compliance and Privacy

**Data Handling Procedures**
- AIS data is public information under international maritime law
- No personal identification information processing
- Vessel tracking limited to commercial maritime activities
- Data retention policies aligned with research requirements

## Performance Metrics and Monitoring

### System Performance Indicators

**Data Collection Metrics**
- Message processing rate (messages per second)
- Database insertion success rate and latency
- WebSocket connection stability and reconnection frequency
- Geographic filtering efficiency and accuracy

**Analysis Performance Metrics**
- Data processing throughput for batch operations
- Query response times for complex analytical queries
- Visualization generation time for interactive outputs
- Memory usage patterns for large dataset processing

### Quality Assurance Metrics

**Data Quality Indicators**
- Coordinate validation success rate
- IMO number validation and MMSI mapping accuracy
- Temporal data consistency and ordering validation
- Duplicate detection and handling effectiveness

**Analysis Accuracy Metrics**
- Scrubber identification precision and recall rates
- Pollution calculation accuracy against regulatory standards
- Forecasting model performance metrics (MAE, RMSE)
- Geospatial analysis precision for boundary detection 