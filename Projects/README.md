# Data Engineering Projects

A collection of comprehensive data engineering projects covering real-time streaming analytics, custom frameworks, IoT automation, and big data processing solutions.

## 📁 Projects Overview

```
Projects/
├── 🚕 ride_app_streaming_analytics/     # Real-time taxi analytics platform
├── ⚡ spark_custom_stats/               # Custom Spark framework with REST API
├── 🔄 kafka_structured_steaming/        # Kafka-Spark streaming pipeline
└── 🏠 Think_Home/                      # IoT smart home automation system
```

## 🚕 Ride App Streaming Analytics

**Real-time streaming analytics platform for taxi trip data**

### Technology Stack
- **Apache Kafka**: Real-time data streaming and message queuing
- **Apache Spark**: Structured streaming for real-time processing
- **Apache Airflow**: Data generation and workflow orchestration
- **Apache Cassandra**: Scalable NoSQL database for analytics storage
- **Grafana**: Real-time monitoring and dashboard visualization
- **Docker**: Containerized deployment and infrastructure management

### Key Features
- **Real-time Data Generation**: Apache Airflow generates realistic taxi trip data
- **Streaming Analytics**: Real-time processing with windowed aggregations
- **Dynamic Pricing**: Demand-based pricing optimization
- **Hotspot Analysis**: Geographic demand pattern identification
- **Comprehensive Monitoring**: Grafana dashboards for real-time insights
- **Fault Tolerance**: Automatic recovery with checkpoint management

### Use Cases
- Real-time fare analytics and revenue tracking
- Trip demand hotspots by location and community areas
- Dynamic pricing based on demand patterns
- Daily trip statistics and trend analysis

---

## ⚡ Spark Custom Framework

**Custom Spark framework with REST API capabilities and database integration**

### Technology Stack
- **Apache Spark**: Distributed computing and data processing
- **Flask**: REST API framework for Spark operations
- **MySQL**: Database for framework statistics and metadata
- **AWS S3**: Cloud storage integration and utilities

### Key Features
- **RESTful API**: HTTP endpoints for Spark job management
- **S3 Utilities**: Bucket exploration and path management
- **Database Integration**: MySQL-based statistics tracking
- **Job Monitoring**: Comprehensive job status and performance tracking
- **Framework Statistics**: Detailed metrics for Spark operations

### Components
- `spark_rest_api.py`: REST API implementation for Spark operations
- `print_all_s3_paths.py`: S3 bucket exploration utilities
- `db_script.py`: Database connection and operations

### Use Cases
- Spark job submission and monitoring via REST API
- S3 bucket management and exploration
- Framework performance tracking and optimization
- Automated Spark workflow management

---

## 🔄 Kafka Structured Streaming

**Real-time data streaming with Apache Spark Structured Streaming**

### Technology Stack
- **Apache Kafka**: Message broker for real-time data streaming
- **Apache Spark**: Structured streaming for data processing
- **Python**: Primary programming language

### Key Features
- **Kafka Integration**: Seamless integration with Spark Structured Streaming
- **Real-time Processing**: Stream processing with micro-batch operations
- **Data Pipeline**: End-to-end streaming data pipeline
- **Test Data Generation**: Automated test data creation and processing

### Components
- `spark_stream.py`: Main streaming application
- `test_file.txt`: Test data for streaming pipeline

### Use Cases
- Real-time data ingestion and processing
- Streaming analytics and transformations
- Event-driven data pipeline development
- Real-time monitoring and alerting

---

## 🏠 Think Home - Smart Home Automation

**IoT-based smart home automation system for lighting and fan control**

### Technology Stack
- **Arduino**: Microcontroller platform for IoT automation
- **PIR Sensors**: Motion detection for room occupancy
- **IR Sensors**: Proximity detection for bathroom automation
- **Relay Modules**: Electrical control for lighting and fan systems

### Key Features
- **Automatic Room Lighting**: PIR sensor-based bedroom lighting control
- **Bathroom Automation**: IR sensor-based bathroom lighting
- **Fan Control**: Manual and automatic fan control system
- **Multi-room Independence**: Independent control for each room
- **Serial Monitoring**: Real-time system status monitoring
- **Safety Mechanisms**: 4-second delay to prevent rapid switching

### Hardware Components
- **Bedroom 1 & 2**: PIR sensors for motion detection
- **Bathroom 1 & 2**: IR sensors for proximity detection
- **Fan System**: Manual control with relay automation
- **Relay Modules**: 4 relay modules for lighting control

### Use Cases
- Smart home lighting automation
- Energy-efficient room management
- Occupancy-based automation
- Home security and monitoring

---

## 🛠️ Technology Matrix

| Project | Spark | Kafka | Airflow | Cassandra | IoT | REST API | Cloud |
|---------|-------|-------|---------|-----------|-----|----------|-------|
| Ride App Analytics | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Spark Custom Framework | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ |
| Kafka Streaming | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Think Home | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ |

## 🎯 Learning Objectives

### Real-Time Processing
- **Ride App Analytics**: Complete real-time streaming pipeline
- **Kafka Streaming**: Basic streaming concepts and implementation

### Big Data Frameworks
- **Spark Custom Framework**: Custom Spark development and API integration
- **Ride App Analytics**: Advanced Spark Structured Streaming

### IoT and Automation
- **Think Home**: Hardware integration and sensor-based automation

### Infrastructure Management
- **Ride App Analytics**: Docker-based deployment and monitoring
- **Spark Custom Framework**: Cloud integration and database management

## 🚀 Getting Started

### Prerequisites
- **Python 3.x**: Required for all Python-based projects
- **Apache Spark**: For Spark-related projects
- **Docker & Docker Compose**: For containerized deployments
- **MySQL Database**: For database integration projects
- **AWS S3 Access**: For cloud storage projects
- **Arduino IDE**: For IoT automation project

### Quick Start Guide
1. **Choose a Project**: Select based on your learning objectives
2. **Check Prerequisites**: Ensure all required software is installed
3. **Follow Project README**: Each project has detailed setup instructions
4. **Start with Simple**: Begin with Kafka Streaming or Think Home for basics
5. **Scale Up**: Move to Ride App Analytics for comprehensive experience

## 📖 Project Complexity Levels

### 🟢 Beginner
- **Think Home**: Hardware and basic automation concepts
- **Kafka Streaming**: Basic streaming implementation

### 🟡 Intermediate
- **Spark Custom Framework**: API development and database integration

### 🔴 Advanced
- **Ride App Analytics**: Full-stack real-time analytics platform

## 🔗 Related Resources

- **Medium Blogs**: Check the `medium_blogs` folder for Apache Spark tutorials
- **Individual READMEs**: Each project contains detailed documentation
- **Docker Compose**: Infrastructure setup for containerized projects
- **Requirements Files**: Python dependencies for each project

---

**Note**: Each project folder contains its own detailed README with specific setup instructions, requirements, and usage examples. Start with the project that best matches your current skill level and learning goals. 