# Music Streaming ETL Pipeline

## Overview

This project implements an **ETL (Extract, Transform, Load) pipeline** for a music streaming service using **Apache Airflow** for orchestration and **AWS Glue** for data transformation. The pipeline processes streaming data arriving at unpredictable intervals, validates and transforms it, and computes key metrics before storing the results in **Amazon DynamoDB** for consumption by downstream applications.

![Architecture Diagram](/assets/images/data%20pipeline%20architecture.png)

## Features

- **Automated Data Ingestion**: Listens for new data files in Amazon S3 using an **S3 Key Sensor**.
- **Manifest-based Data Validation**: Uses a `manifest.json` file stored in S3 to validate schema, enforce field requirements, and handle errors.
- **Data Transformation**: Processes and transforms data using AWS Glue.
- **Key Performance Indicators (KPIs) Computation**:
  - Total listens per genre per day
  - Unique listeners per genre per day
  - Total and average listening time per user
  - Top 3 songs per genre per day
  - Top 5 genres per day
- **Data Storage**: Stores transformed data in **Amazon DynamoDB** for fast access.
- **Error Handling & Logging**: Implements detailed logging, validation, and error-handling mechanisms.
- **Archival & Cleanup**: Moves processed files to an archive directory and handles failed files by moving them to quarantine.

## Architecture

1. **Data Ingestion**: Streaming data is uploaded to **Amazon S3** in CSV format.
2. **Airflow Orchestration**: An **Apache Airflow DAG** orchestrates the ETL workflow.
3. **Manifest-based Data Validation**: A `manifest.json` file stored in the S3 bucket defines schema rules, required fields, and validation policies. The pipeline reads this file to enforce schema compliance before processing data.
4. **Data Transformation**: AWS Glue processes data using PySpark and Python Shell jobs.
5. **KPI Computation**: Key metrics are computed and prepared for storage.
6. **Data Storage**: Processed data is written to **Amazon DynamoDB** for fast retrieval.
7. **Archival & Cleanup**: Successfully processed files are moved to an archive, while failed files are quarantined.

## Installation

### Prerequisites

- **Apache Airflow** installed and configured.
- **AWS Credentials** set up with access to S3, Glue, and DynamoDB.
- Python dependencies installed:
  ```sh
  pip install apache-airflow apache-airflow-providers-amazon pandas boto3
  ```

### Setup

1. **Clone the repository**:
   ```sh
   git clone https://github.com/your-repo/music-streaming-etl.git
   cd music-streaming-etl
   ```
2. **Configure Airflow Variables**:
   - Set up **Airflow Variables** for S3 bucket, source paths, and AWS configurations.
   ```json
   {
     "bucket": "your-s3-bucket-name",
     "source_path": "data/streams/"
   }
   ```
3. **Ensure `manifest.json` is stored in S3**:
   - Example manifest file:
   ```json
   {
     "name": "manifest.json",
     "version": "1.0",
     "directory": "data/streams/",
     "purpose": "Store incoming streaming data.",
     "schema": {
       "extension": ".csv",
       "max_rows": 11346,
       "name_pattern": "streams[0-9]+.csv",
       "fields_required": true,
       "fields": {
         "user_id": { "type": "int", "required": true },
         "track_id": { "type": "string", "required": true },
         "listen_time": { "type": "timestamp", "required": true }
       },
       "validation": {
         "on_error": "quarantine",
         "quarantine_path": "data/quarantine"
       }
     }
   }
   ```
4. **Start Airflow**:
   ```sh
   airflow scheduler & airflow webserver
   ```

## Usage

1. Upload streaming data files (CSV) to the **Amazon S3 bucket**.
2. Ensure that the `manifest.json` file exists in the same S3 directory as the streaming files.
3. Upload or copy streaming_transformation.py to your aws-glue-scripts directory.
4. Trigger the **Airflow DAG** manually or wait for it to detect new files.
5. Monitor DAG execution in the **Airflow UI**.
6. Processed data will be stored in **Amazon DynamoDB**.
7. Retrieve insights using queries on DynamoDB.

![Dag Run](/assets/images/dag.png)

## DAG Workflow

- **`S3KeySensor`**: Waits for new streaming data files.
- **`validate_streaming_files_task`**: Reads and enforces schema from `manifest.json`.
- **`prepare_files_for_job_run_task`**: Prepares files for Glue job.
- **`run_glue_job_task`**: Executes the AWS Glue transformation job.
- **`archive_and_cleanup_task`**: Moves processed files to an archive.
- **`cleanup_on_failure_task`**: Moves failed files to quarantine if validation fails.

## Error Handling

- Validation rules are defined in `manifest.json`. If validation fails, files are moved to the quarantine path specified in the manifest.
- Failed validations trigger the **cleanup_on_failure_task**, which moves files to a quarantine folder.
- Logs are maintained for debugging in case of pipeline failures.
- Retries are enabled for certain tasks to handle transient failures.

## Contributions

1. Fork the repository.
2. Create a new feature branch: `git checkout -b feature-name`.
3. Commit your changes: `git commit -m 'Add new feature'`.
4. Push to the branch: `git push origin feature-name`.
5. Open a Pull Request.

## License

This project is licensed under the **MIT License**.

## Contact

For questions, reach out to **Me** at `sannimarzuk@gmail.com`.
