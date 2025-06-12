# 🚕 Ride App Analytics - Real-Time Streaming Platform

A comprehensive real-time analytics platform for taxi trip data using Apache Kafka, Apache Spark, Apache Airflow, and Apache Cassandra. This project demonstrates end-to-end data streaming with real-time processing, analytics, and storage capabilities.

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Components](#components)
- [Usage](#usage)
- [Monitoring](#monitoring)
- [Development](#development)
- [Troubleshooting](#troubleshooting)

## 🎯 Project Overview

This project implements a real-time streaming analytics platform that:

- **Generates** realistic taxi trip data using Apache Airflow
- **Streams** data through Apache Kafka for real-time ingestion
- **Processes** data using Apache Spark Structured Streaming
- **Stores** analytics results in Apache Cassandra
- **Monitors** the entire pipeline through various UIs

The platform provides real-time insights into:
- Fare analytics and revenue tracking
- Trip demand hotspots by location and community areas
- Dynamic pricing based on demand patterns
- Daily trip statistics and trends

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Apache        │───▶│   Apache        │───▶│   Kafka UI      │
│   Airflow       │    │   Kafka         │    │   (Management)  │
│   (Data Gen)    │    │   (Streaming)   │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Grafana       │◀───│   Apache        │◀───│   Apache        │
│   (Monitoring)  │    │   Cassandra     │    │   Spark         │
│                 │    │   (Storage)     │    │   (Processing)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## ✨ Features

### 🔄 Real-Time Data Processing
- **Structured Streaming**: Apache Spark processes data in real-time with windowed aggregations
- **Watermarking**: Handles late-arriving data with configurable watermarks
- **Micro-batch Processing**: Efficient processing with configurable triggers
- **Fault Tolerance**: Automatic recovery with checkpoint management

### 📊 Analytics Functions
1. **Fare Analytics** (`sumFareWindowStream`)
   - 5-second window aggregations of fares, tips, and trip totals
   - Real-time revenue tracking and trend analysis
   - Configurable watermarking for late data handling

2. **Trip Statistics** (`tripsTotalStream`)
   - Daily trip counts and averages
   - Current day performance metrics
   - Real-time trip volume monitoring

3. **Community Hotspots** (`hotspotCommunityPickupWindowStream`)
   - Demand analysis by community areas
   - 1-minute sliding windows with 30-second overlap
   - **✅ Cassandra Integration Implemented**
   - Geographic demand pattern identification

4. **Location Hotspots** (`hotspotWindowStream`)
   - Geographic demand hotspots with lat/lon precision
   - Dynamic pricing based on demand patterns
   - Surcharge calculation for high-demand areas (20% premium for demand > 2)
   - Real-time pricing optimization

### 🗄️ Data Storage
- **Apache Cassandra**: Scalable NoSQL database for analytics results
- **Kafka Topics**: Real-time data streaming and message queuing
- **Checkpoint Management**: Fault-tolerant stream processing
- **Data Persistence**: Reliable storage with replication

## 📋 Prerequisites

Before running this project, ensure you have:

- **Docker** (version 20.10 or higher)
- **Docker Compose** (version 2.0 or higher)
- **At least 8GB RAM** available for Docker
- **At least 4 CPU cores** recommended
- **At least 20GB free disk space**

### System Requirements
- **Operating System**: Windows 10/11, macOS, or Linux
- **Memory**: 8GB RAM minimum (16GB recommended)
- **Storage**: 20GB free space
- **Network**: Internet connection for Docker image downloads

## 🚀 Quick Start

### 1. Clone and Navigate
```bash
git clone <your-repository-url>
cd ride_app_analytics
```

### 2. Start the Infrastructure
```bash
# Start all services
docker-compose up -d

# Wait for services to be healthy (2-3 minutes)
docker-compose ps
```

### 3. Access the Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow Web UI** | http://localhost:8080 | `airflow` / `airflow` |
| **Kafka UI** | http://localhost:8085 | No authentication |
| **Grafana** | http://localhost:3000 | `admin` / `admin` |
| **Spark Master** | http://localhost:9090 | No authentication |

### 4. Run the Analytics Pipeline

#### Option A: Using Airflow (Recommended)
1. Open Airflow UI at http://localhost:8080
2. Navigate to DAGs
3. Enable the `produce_taxi_data` DAG
4. Trigger the DAG to start data generation

#### Option B: Direct Script Execution
```bash
# Execute the main analytics script
docker exec -it spark-master python /path/to/main_script.py
```

## 📁 Project Structure

```
ride_app_analytics/
├── 📁 dags/                          # Apache Airflow DAGs
│   └── produce_taxi_data_dag.py     # Taxi data generation DAG
├── 📁 scripts/                       # Main application scripts
│   └── main_script.py               # Spark streaming analytics
├── 📁 screenshots/                   # Project documentation images
│   ├── Airflow_Dag.png             # Airflow DAG visualization
│   ├── Cassandra_Table.png         # Cassandra table structure
│   ├── Console_Output.png          # Application output
│   └── Grafana_Dashboard.png       # Grafana monitoring dashboard
├── 📄 docker-compose.yaml           # Infrastructure orchestration
├── 📄 requirements.txt              # Python dependencies
├── 📄 dependencies.zip              # Pre-built dependencies
└── 📄 README.md                     # This file
```

## 🔧 Components

### 1. Apache Airflow (Data Generation)
- **Purpose**: Generates realistic taxi trip data
- **Port**: 8080
- **DAG**: `produce_taxi_data`
- **Features**:
  - Random trip data generation with realistic parameters
  - Geographic coordinates based on Chicago area (41.6-42.1°N, 87.5-87.9°W)
  - Multiple taxi companies (Alpha Cabs, Beta Taxi, City Rides, etc.)
  - Various payment types (Credit Card, Cash, No Charge, Dispute)
  - Configurable data volume and frequency (0.2-second intervals)
  - 30-day historical data simulation

### 2. Apache Kafka (Message Broker)
- **Purpose**: Real-time data streaming
- **Port**: 9092 (broker), 8085 (UI)
- **Topics**: `taxi_trips`
- **Features**:
  - High-throughput message queuing (1000+ messages/second)
  - Fault-tolerant streaming with replication
  - Web-based management interface (Kafka UI)
  - Real-time topic monitoring and message inspection
  - Consumer group management

### 3. Apache Spark (Data Processing)
- **Purpose**: Real-time analytics and transformations
- **Port**: 9090 (master), 4040 (application)
- **Features**:
  - Structured streaming with micro-batch processing
  - Windowed aggregations (5-second, 1-minute windows)
  - Real-time analytics with watermarking
  - Fault tolerance with checkpoint management
  - Multi-worker cluster support
  - Real-time query monitoring

### 4. Apache Cassandra (Data Storage)
- **Purpose**: Analytics results storage
- **Port**: 9042
- **Keyspace**: `spark_streaming`
- **Features**:
  - Scalable NoSQL storage with linear scalability
  - High write throughput (10,000+ writes/second)
  - Distributed architecture with replication
  - ACID compliance for analytics data
  - Automatic data distribution and load balancing

### 5. Grafana (Monitoring)
- **Purpose**: System monitoring and visualization
- **Port**: 3000
- **Features**:
  - Real-time dashboards with customizable panels
  - System metrics and performance monitoring
  - Custom visualizations for analytics data
  - Alerting and notification capabilities
  - Multi-data source support

## 📊 Usage

### Starting the Pipeline

1. **Start Infrastructure**:
   ```bash
   docker-compose up -d
   ```

2. **Monitor Service Health**:
   ```bash
   docker-compose ps
   ```

3. **Generate Data** (via Airflow):
   - Open http://localhost:8080
   - Enable `produce_taxi_data` DAG
   - Trigger the DAG

4. **Run Analytics**:
   ```bash
   # The main script will automatically start processing
   # Monitor logs for real-time analytics
   ```

### Data Flow

1. **Data Generation**: Airflow generates taxi trip data every 0.2 seconds with realistic parameters
2. **Kafka Ingestion**: Data streams into `taxi_trips` topic with JSON format
3. **Spark Processing**: Four parallel analytics streams process the data:
   - **Fare Analytics**: 5-second window aggregations of fares, tips, and totals
   - **Trip Statistics**: Daily trip counts and averages with current day filtering
   - **Community Hotspots**: 1-minute sliding windows for community area demand analysis
   - **Location Hotspots**: Geographic demand with dynamic pricing (20% surcharge for high demand)
4. **Storage**: Results stored in Cassandra (community hotspots) and Kafka topics

### Analytics Output

The system generates four types of analytics:

1. **Fare Analytics**: Real-time fare, tips, and total aggregation with 5-second windows
2. **Trip Statistics**: Daily trip counts and averages with current day performance tracking
3. **Community Hotspots**: Demand by community areas with Cassandra storage and UUID tracking
4. **Location Hotspots**: Geographic demand with dynamic pricing based on demand patterns

## 📈 Monitoring

### Service Health Checks
```bash
# Check all services
docker-compose ps

# View logs for specific service
docker-compose logs -f airflow-webserver
docker-compose logs -f spark-master
docker-compose logs -f cassandra_db
```

### Web Interfaces

#### Airflow Dashboard
- **URL**: http://localhost:8080
- **Purpose**: Monitor DAG execution, trigger jobs, view logs
- **Key Features**: DAG visualization, task monitoring, log access, workflow management
- **Screenshot**: See `screenshots/Airflow_Dag.png` for DAG structure

#### Kafka UI
- **URL**: http://localhost:8085
- **Purpose**: Monitor Kafka topics, messages, and cluster health
- **Key Features**: Topic browsing, message inspection, cluster metrics, consumer groups
- **Real-time Monitoring**: Live message flow and topic statistics

#### Grafana
- **URL**: http://localhost:3000
- **Purpose**: System monitoring and visualization
- **Key Features**: Custom dashboards, metrics visualization, alerting, performance monitoring
- **Screenshot**: See `screenshots/Grafana_Dashboard.png` for monitoring interface

#### Spark Master
- **URL**: http://localhost:9090
- **Purpose**: Monitor Spark applications and workers
- **Key Features**: Application status, worker health, job monitoring, resource utilization
- **Real-time Metrics**: Live application performance and resource usage

### Monitoring Screenshots

The project includes comprehensive screenshots for monitoring and verification:

- **`Airflow_Dag.png`**: Shows the complete DAG structure and task dependencies
- **`Cassandra_Table.png`**: Displays the Cassandra table structure and data schema
- **`Console_Output.png`**: Captures real-time application output and processing logs
- **`Grafana_Dashboard.png`**: Shows the monitoring dashboard with system metrics

## 🔧 Development

### Adding New Analytics Functions

1. **Create New Function** in `scripts/main_script.py`:
   ```python
   def newAnalyticsFunction(info_df_fin):
       # Your analytics logic here
       # Follow the pattern of existing functions
       pass
   ```

2. **Add Cassandra Integration** (if needed):
   ```python
   # Create table schema
   cassandra_schema = [
       ("id", "uuid"),
       ("column1", "TEXT"),
       # ... more columns
   ]
   
   # Create table
   create_cassandra_table(session, table_name, cassandra_schema, primary_key)
   ```

3. **Register in Main**:
   ```python
   new_stream = newAnalyticsFunction(info_df_fin)
   new_stream.awaitTermination()
   ```

### Customizing Data Generation

Edit `dags/produce_taxi_data_dag.py` to modify:
- Trip frequency (`sleep_duration` - currently 0.2 seconds)
- Geographic boundaries (currently Chicago area)
- Payment types and companies
- Data volume and duration (currently 920 seconds runtime)
- Trip parameters (fare ranges, distance, time)

### Configuration

Key configuration files:
- `docker-compose.yaml`: Service configuration and networking
- `requirements.txt`: Python dependencies and versions
- `scripts/main_script.py`: Analytics configuration and processing parameters

## ⚠️ Important Notes

### Cassandra Integration Status

**⚠️ Assignment Notice**: I have implemented Cassandra writing functionality only for the `hotspotCommunityPickupWindowStream` function. 

**Take it as an assignment to implement for others:**

- `sumFareWindowStream` - Add Cassandra storage for fare analytics
- `tripsTotalStream` - Add Cassandra storage for trip statistics  
- `hotspotWindowStream` - Add Cassandra storage for location hotspots

### Implementation Guidelines

To add Cassandra integration to other functions:

1. **Create Table Schema**:
   ```python
   cassandra_schema = [
       ("id", "uuid"),
       # Define your columns
   ]
   ```

2. **Create Table**:
   ```python
   create_cassandra_table(session, table_name, cassandra_schema, primary_key)
   ```

3. **Modify Processing**:
   ```python
   # Add UUID and timestamp columns
   df = df.withColumn("id", uuid_udf())
   
   # Update write_to_multiple_sinks call
   .foreachBatch(write_to_multiple_sinks(topic, table_name))
   ```

### Performance Considerations

- **Memory Usage**: Each service requires significant memory allocation
- **Processing Windows**: Adjust window sizes based on data volume and latency requirements
- **Checkpoint Management**: Regular cleanup of checkpoint directories
- **Network Latency**: Consider network performance for distributed processing

## 🐛 Troubleshooting

### Common Issues

#### 1. Services Not Starting
```bash
# Check Docker resources
docker system df
docker system prune

# Restart services
docker-compose down
docker-compose up -d
```

#### 2. Memory Issues
```bash
# Increase Docker memory limit
# Docker Desktop → Settings → Resources → Memory → 8GB+
```

#### 3. Port Conflicts
```bash
# Check port usage
netstat -an | grep :8080
netstat -an | grep :9092

# Modify ports in docker-compose.yaml if needed
```

#### 4. Spark Connection Issues
```bash
# Check Spark master logs
docker-compose logs spark-master

# Verify network connectivity
docker exec -it spark-master ping cassandra_db
```

#### 5. Cassandra Connection Issues
```bash
# Check Cassandra logs
docker-compose logs cassandra_db

# Verify keyspace creation
docker exec -it cassandra_db cqlsh -e "DESCRIBE KEYSPACES;"
```

### Log Locations

- **Airflow**: `./logs/` directory
- **Spark**: http://localhost:9090 (Web UI)
- **Kafka**: http://localhost:8085 (Web UI)
- **Cassandra**: `docker-compose logs cassandra_db`
- **Application**: `screenshots/Console_Output.png` shows sample output

### Performance Tuning

1. **Increase Resources**:
   - Docker memory: 8GB+
   - Docker CPUs: 4+
   - Disk space: 20GB+

2. **Adjust Processing**:
   - Modify window sizes in analytics functions
   - Adjust trigger intervals (currently 30 seconds for community hotspots, 1 minute for location hotspots)
   - Tune checkpoint locations

3. **Optimize Data Flow**:
   - Adjust Kafka producer batch sizes
   - Tune Spark executor memory and cores
   - Optimize Cassandra write consistency levels

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Cassandra Documentation](https://cassandra.apache.org/doc/)
- [Grafana Documentation](https://grafana.com/docs/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Implement your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Happy Streaming! 🚀**

For questions or issues, please create an issue in the repository. 