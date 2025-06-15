# Spark Custom Metrics Framework

This repository contains a custom Spark framework with REST API capabilities, database integration, and S3 utilities.
Whenever spark saves any data, it saves
  - Size of entire files written
  - Avg File Size
  - Number of Files saved
  - Count of Records 
  - Time taken to save the data
  - Miscellaneous values like type of files, path, partitioned columns etc.

This will be very helpful to get an overall understanding of how Spark is utilising our Data Lake and for performance Optimisations

## Project Structure

```
.
├── spark_rest_api.py      # REST API implementation for Spark operations
├── print_all_s3_paths.py  # Utility to list S3 paths
└── db_script.py          # Database connection and operations script
```

## Features

### 1. Spark REST API (`spark_rest_api.py`)
- RESTful API endpoints for Spark operations
- Handles Spark job submissions and monitoring
- Supports various Spark operations through HTTP endpoints

### 2. S3 Utilities (`print_all_s3_paths.py`)
- Lists all S3 paths in a given bucket
- Supports filtering and pattern matching
- Useful for S3 bucket exploration and management

### 3. Database Integration (`db_script.py`)
- MySQL database connection management
- Table creation and data operations
- Spark framework statistics tracking

## Prerequisites

- Python 3.x
- Apache Spark
- MySQL Database
- AWS S3 Access
- Required Python packages:
  ```
  pyspark
  mysql-connector-python
  boto3
  flask
  ```

## Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd spark-custom-framework
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure database:
   - Update database credentials in `db_script.py`
   - Create required tables using the script

4. Configure AWS credentials:
   - Set up AWS credentials for S3 access
   - Configure region and bucket names

## Usage

### Starting the REST API
```bash
python spark_rest_api.py
```

### Listing S3 Paths
```bash
python print_all_s3_paths.py --bucket <bucket-name> --prefix <prefix>
```

### Database Operations
```bash
python db_script.py
```

## API Endpoints

The REST API provides the following endpoints:

- `POST /submit-job`: Submit a new Spark job
- `GET /job-status/<job_id>`: Check job status
- `GET /list-jobs`: List all submitted jobs
- `DELETE /cancel-job/<job_id>`: Cancel a running job

## Database Schema

The framework uses a MySQL database with the following main table:

```sql
CREATE TABLE spark_framework_stats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    process_name VARCHAR(50),
    sql_id VARCHAR(50),
    sql_status VARCHAR(15),
    src_paths VARCHAR(100),
    src_file_formats VARCHAR(50),
    src_no_of_files INT UNSIGNED,
    src_size_in_gb VARCHAR(15),
    src_counts BIGINT UNSIGNED,
    dest_path VARCHAR(100),
    dest_no_of_files INT UNSIGNED,
    dest_size_in_gb VARCHAR(15),
    dest_count BIGINT UNSIGNED,
    dest_schema VARCHAR(200),
    dest_file_format VARCHAR(15),
    dest_num_partitions INT UNSIGNED,
    dest_partition_cols VARCHAR(15),
    dest_repartition_no INT UNSIGNED,
    duration_in_mins SMALLINT UNSIGNED,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    response VARCHAR(200)
);
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support, please open an issue in the GitHub repository or contact the maintainers. 
