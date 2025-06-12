# Data Engineering Repository

A comprehensive collection of data engineering projects, tutorials, and implementations covering Apache Spark, streaming analytics, IoT automation, and real-time data processing.

## 📁 Repository Structure

```
data_eng/
├── 📁 medium_blogs/                    # Blogs on Data Engineering & other tutorials
│   ├── Custom Job Description in Spark/
│   └── 02_Spark_Caching_CSV_vs_Parq/
└── 📁 Projects/                        # Full-stack data engineering projects
    ├── ride_app_streaming_analytics/
    ├── spark_custom_stats/
    ├── kafka_structured_steaming/
    └── Think_Home/
```

## 📚 Medium Blogs
https://medium.com/@praveenbn88
### Apache Spark Tutorials and Examples

The `medium_blogs` folder contains practical Apache Spark examples and tutorials:

#### 1. Custom Job Description in Spark
- **File**: `custom_job_description.py`
- **Purpose**: Demonstrates how to set custom job descriptions in Apache Spark for better monitoring and debugging
- **Key Features**:
  - Shows how to use `sc.setJobDescription()` for custom job naming
  - Demonstrates job description persistence across actions
  - Includes examples of resetting job descriptions to default values
  - Useful for Spark UI monitoring and job tracking

#### 2. Spark Caching: CSV vs Parquet
- **File**: `02_Spark_Caching_CSV_vs_Parq.py`
- **Purpose**: Performance comparison between CSV and Parquet file formats in Spark caching
- **Key Features**:
  - Demonstrates memory usage differences between CSV and Parquet caching
  - Shows data type casting impact on memory consumption
  - Includes practical examples with 12.5M records
  - Performance optimization insights for Spark applications

## 🚀 Projects

### Full-Stack Data Engineering Implementations

The `Projects` folder contains comprehensive data engineering solutions:

#### 1. Ride App Streaming Analytics
- **Technology Stack**: Apache Kafka, Apache Spark, Apache Airflow, Apache Cassandra, Grafana
- **Purpose**: Real-time streaming analytics platform for taxi trip data
- **Key Features**:
  - Real-time data generation using Apache Airflow
  - Streaming data processing with Apache Spark Structured Streaming
  - Real-time analytics including fare tracking, trip statistics, and demand hotspots
  - Dynamic pricing based on demand patterns
  - Comprehensive monitoring with Grafana dashboards
  - Docker-based deployment with complete infrastructure orchestration

#### 2. Spark Custom Framework
- **Technology Stack**: Apache Spark, Flask, MySQL, AWS S3
- **Purpose**: Custom Spark framework with REST API capabilities and database integration
- **Key Features**:
  - RESTful API for Spark operations (`spark_rest_api.py`)
  - S3 utilities for bucket exploration (`print_all_s3_paths.py`)
  - MySQL database integration for framework statistics (`db_script.py`)
  - Comprehensive job monitoring and management
  - Database schema for tracking Spark framework performance metrics

#### 3. Kafka Structured Streaming
- **Technology Stack**: Apache Kafka, Apache Spark
- **Purpose**: Real-time data streaming with Apache Spark Structured Streaming
- **Key Features**:
  - Kafka integration with Spark Structured Streaming
  - Real-time data processing pipeline
  - Streaming analytics implementation
  - Test data generation and processing

#### 4. Think Home - Smart Home Automation
- **Technology Stack**: Arduino, IoT Sensors (PIR, IR)
- **Purpose**: Smart home automation system for lighting and fan control
- **Key Features**:
  - Automatic room lighting control using PIR sensors
  - Bathroom lighting control using IR sensors
  - Fan control system with manual override
  - Serial monitoring for system status
  - Multi-room independent control
  - 4-second delay mechanism to prevent rapid switching

## 🛠️ Technologies Used

### Big Data & Streaming
- **Apache Spark**: Distributed computing and real-time processing
- **Apache Kafka**: Real-time data streaming and message queuing
- **Apache Airflow**: Workflow orchestration and data pipeline management
- **Apache Cassandra**: Scalable NoSQL database for real-time analytics

### Cloud & Storage
- **AWS S3**: Cloud storage and data lake solutions
- **MySQL**: Relational database for metadata and statistics

### Monitoring & Visualization
- **Grafana**: Real-time monitoring and dashboard creation
- **Kafka UI**: Kafka cluster management and monitoring

### IoT & Hardware
- **Arduino**: Microcontroller platform for IoT automation
- **PIR Sensors**: Motion detection for room occupancy
- **IR Sensors**: Proximity detection for bathroom automation
- **Relay Modules**: Electrical control for lighting and fan systems

### Development & APIs
- **Flask**: REST API framework for Spark operations
- **Docker**: Containerization for consistent deployment
- **Python**: Primary programming language for data engineering

## 🎯 Use Cases

This repository covers various data engineering scenarios:

1. **Real-Time Analytics**: Streaming data processing and real-time insights
2. **Big Data Processing**: Large-scale data transformations and analytics
3. **IoT Data Integration**: Sensor data collection and automation
4. **Performance Optimization**: Caching strategies and format comparisons
5. **Infrastructure Management**: Containerized deployment and monitoring
6. **API Development**: RESTful services for data operations

## 🚀 Getting Started

### Prerequisites
- Python 3.x
- Apache Spark
- Docker and Docker Compose
- MySQL Database
- AWS S3 Access (for relevant projects)
- Arduino IDE (for IoT project)

### Quick Start
1. Clone the repository
2. Navigate to specific project folders for detailed setup instructions
3. Follow individual README files for project-specific requirements
4. Use Docker Compose for containerized projects

## 📖 Learning Path

1. **Begin with Medium Blogs**: Start with Spark tutorials for fundamental concepts
2. **Explore Individual Projects**: Each project demonstrates different aspects of data engineering
3. **Combine Technologies**: Projects show how to integrate multiple technologies
4. **Scale and Optimize**: Learn performance optimization and monitoring

## 🤝 Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for any improvements or bug fixes.

## 📄 License

This project is licensed under the MIT License - see individual project folders for specific license details.

---

**Note**: Each project folder contains its own detailed README with specific setup instructions, requirements, and usage examples. 