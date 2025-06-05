# Service Management Guide: AIS Data Collection System

## Service Management Overview

The AIS Data Collection System provides multiple interfaces for managing the collection and processing services. The system includes automated management scripts, manual control interfaces, and comprehensive monitoring capabilities.

## Primary Management Interface

### Automated Service Management (`manage_services.py`)

The primary management interface provides comprehensive control over all system components through a unified command-line interface.

**Starting Services**
```bash
python manage_services.py start
```
This command builds Docker images, starts all services, and provides status verification. The startup process includes the AIS Collector, Length-Width Correction Service, and Cloud SQL Proxy with automatic dependency resolution.

**Stopping Services**
```bash
python manage_services.py stop
```
Gracefully stops all running services while preserving data integrity and ensuring proper cleanup of Docker resources.

**Service Status Monitoring**
```bash
python manage_services.py status
```
Displays current status of all services including container health, resource utilization, and connectivity status.

**Log Monitoring**
```bash
python manage_services.py logs --follow
python manage_services.py logs --service ais_collector --follow
```
Provides real-time log streaming for system monitoring and troubleshooting. Supports service-specific log filtering and continuous monitoring modes.

**System Cleanup**
```bash
python manage_services.py clean
```
Performs comprehensive system cleanup including container removal, image cleanup, and volume management for maintenance and troubleshooting.

### Manual Control Scripts

**Direct Service Startup**
```bash
./start_services.sh
```
Shell script for direct Docker Compose management without Python dependencies. Provides basic service startup with Docker environment verification.

**Direct Service Shutdown**
```bash
./stop_services.sh
```
Direct service termination with Docker Compose management for emergency shutdown or maintenance scenarios.

## Service Architecture and Dependencies

### Service Component Overview

**Cloud SQL Proxy Service**
- Container: `cloud_sql_proxy`
- Function: Secure database connection management
- Dependencies: Google Cloud service account credentials
- Network: Internal Docker network on port 5432

**AIS Collector Service**
- Container: `ais_collector`
- Function: Real-time AIS data collection and database insertion
- Dependencies: Cloud SQL Proxy, AISStream.io API access
- Data Flow: WebSocket → Processing → Database

**Length-Width Correction Service**
- Container: `lw_correction_service`
- Function: Continuous data quality monitoring and correction
- Dependencies: Cloud SQL Proxy, ships database table
- Operation: Automated dimension validation and correction

### Service Interdependencies

**Startup Sequence**
1. Cloud SQL Proxy initialization and database connectivity verification
2. AIS Collector startup with WebSocket connection establishment
3. Length-Width Correction Service activation for data quality monitoring

**Shutdown Sequence**
1. Graceful termination of data collection processes
2. Completion of pending database transactions
3. Service cleanup and resource deallocation

## Configuration Management

### Environment Configuration

**Database Connection Parameters**
- `DB_NAME`: PostgreSQL database name for AIS data storage
- `DB_USER`: Database user credentials for service authentication
- `DB_PASSWORD`: Secure password management for database access
- `DB_HOST`: Internal Docker network hostname for database connectivity
- `DB_PORT`: Database port configuration for service communication

**External Service Integration**
- `API_KEY`: AISStream.io API authentication for data access
- Service account JSON: Google Cloud SQL authentication credentials

### Docker Compose Configuration

**Service Definition**
The `docker-compose.yml` file defines service configurations, network topology, and volume management for the complete system architecture.

**Network Architecture**
Internal Docker networking provides secure service communication while isolating external access points for security optimization.

**Volume Management**
Persistent volume mapping for configuration files, credentials, and log storage with appropriate security permissions.

## Monitoring and Troubleshooting

### Real-time Monitoring

**Container Health Monitoring**
```bash
docker-compose ps
docker stats
```
Provides real-time container status, resource utilization, and health indicators for system monitoring.

**Database Connection Monitoring**
Connection health verification through proxy status monitoring and database query responsiveness testing.

**Data Collection Monitoring**
AIS message processing rates, database insertion success rates, and WebSocket connection stability monitoring.

### Log Analysis and Troubleshooting

**Service-Specific Log Analysis**
- AIS Collector: WebSocket connection status, message processing rates, database insertion success
- Length-Width Correction: Data quality metrics, correction frequency, processing statistics
- Cloud SQL Proxy: Connection health, authentication status, network connectivity

**Error Pattern Recognition**
Common error patterns include WebSocket disconnections, database connection failures, and API rate limiting scenarios.

**Performance Monitoring**
Database query performance, memory utilization patterns, and processing throughput monitoring for optimization.

### Common Issues and Solutions

**WebSocket Connection Issues**
- Verify API key validity and rate limiting status
- Check network connectivity and firewall configurations
- Monitor AISStream.io service status and availability

**Database Connection Problems**
- Validate Cloud SQL Proxy configuration and credentials
- Verify database instance availability and network access
- Check service account permissions and authentication

**Data Quality Issues**
- Monitor Length-Width Correction Service logs for processing statistics
- Verify reference data integrity and update procedures
- Check data validation rules and processing algorithms

## Maintenance Procedures

### Regular Maintenance Tasks

**Log Management**
Periodic log rotation and archival to manage disk space utilization and maintain system performance.

**Database Maintenance**
Regular database optimization including index maintenance, statistics updates, and query performance analysis.

**Credential Management**
Periodic rotation of API keys, database credentials, and service account keys following security best practices.

### System Updates and Upgrades

**Docker Image Updates**
Procedure for updating base images and application dependencies while maintaining data continuity and service availability.

**Database Schema Updates**
Migration procedures for database schema changes with backup and rollback capabilities for data protection.

**Configuration Updates**
Safe procedures for updating service configurations with testing and validation protocols.

### Backup and Recovery

**Data Backup Procedures**
Cloud SQL automated backup configuration and manual backup procedures for critical data protection.

**Service Configuration Backup**
Backup procedures for service configurations, credentials, and deployment specifications.

**Recovery Procedures**
Step-by-step recovery procedures for various failure scenarios including partial service failures and complete system recovery.

## Performance Optimization

### Resource Management

**Memory Optimization**
Container memory allocation optimization based on workload patterns and processing requirements.

**CPU Resource Allocation**
Processing capacity allocation for data collection, quality correction, and database operations.

**Network Optimization**
Network configuration optimization for WebSocket connectivity and database communication efficiency.

### Scaling Considerations

**Horizontal Scaling**
Procedures for scaling collection capacity through multiple collector instances and load distribution.

**Database Scaling**
Database performance optimization through indexing, partitioning, and query optimization strategies.

**Geographic Scaling**
Considerations for expanding geographic coverage and managing multiple collection regions.

## Security Management

### Access Control

**Service Account Security**
Principle of least privilege implementation for service account permissions and regular access audits.

**Network Security**
Container network isolation and external access restriction for security optimization.

**Credential Security**
Secure credential storage and rotation procedures for API keys and database authentication.

### Monitoring and Auditing

**Security Event Monitoring**
Monitoring for unauthorized access attempts, unusual data access patterns, and security policy violations.

**Audit Trail Management**
Comprehensive logging of security events, access patterns, and administrative actions for compliance and security analysis.

**Compliance Procedures**
Procedures for maintaining compliance with maritime data regulations and institutional security requirements. 