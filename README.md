# 10Alytics-Job-Board-Service-ETL-Pipeline

## Data Pipeline with AWS Services and Redshift
Despite 10Alytics' success in providing leading-edge technology training services, a critical gap exists in the journey of our trainees as they transition from acquiring skills to securing meaningful employment. The current absence of a dedicated job placement system hinders the seamless integration of our trained individuals into the workforce.

To address this challenge, this project aims to construct a comprehensive data infrastructure solution. This solution will facilitate the creation of a dynamic job board, ensuring that our trainees receive daily updates on relevant job opportunities, ultimately enhancing their chances of successful career placement in the rapidly evolving tech industry.

This project implements a data pipeline using various AWS services like S3 buckets, Redshift, and Python libraries like Boto3 and psycopg2. The pipeline involves extracting data from an API, storing it in an S3 bucket, transforming the data, and finally loading it into a Redshift data warehouse.

### Project Structure
- index.py: Main script that orchestrates the data pipeline.
- utils/: Contains utility functions for interacting with AWS services and performing data transformations.
- sql_statements/: SQL statements for creating tables in the Redshift database.
requirements.txt: List of Python dependencies for the project.

### Functionality
1. Creating S3 Buckets:
The script creates S3 buckets for storing raw and transformed data.

2. Fetching Data from API:
Retrieves data from an API endpoint using the provided API key and uploads it to the raw data bucket on S3.

3. Transforming Data:
Transforms the raw data stored in the S3 bucket and uploads the transformed data to a separate bucket.

4. Loading Data into Redshift:
Establishes a connection to the Redshift data warehouse and creates tables necessary for storing the transformed data. Copies the data from the S3 bucket into the Redshift tables.


## Airflow DAG Configuration
This project also includes an Airflow Directed Acyclic Graph (DAG) for orchestrating the data pipeline using scheduled tasks.

### DAG Configuration
- DAG Name: Jobs data
- Description: 10Alytics Job Boards Data
- Schedule: Runs daily (@daily)
- Start Date: January 7, 2024

### Tasks
- begin_execution: Dummy task to mark the beginning of the DAG execution.
- extract_data: PythonOperator to extract data from the API and upload it to the S3 bucket.
- transform_data: PythonOperator to transform the data and upload it to another S3 bucket.
- copy_to_redshift: PythonOperator to copy data from the S3 bucket to Redshift.
- end_execution: Dummy task to mark the end of the DAG execution.

### Task Dependencies
- begin_execution >> extract_data: Start the execution and extract data.
- extract_data >> transform_data: Transform the extracted data.
- transform_data >> copy_to_redshift: Copy transformed data to Redshift.
- copy_to_redshift >> end_execution: Finish the execution.

### DAG Dependencies
- The DAG jobs_data depends on the successful completion of each task before proceeding to the next task.
- If a task fails, it will retry once after a delay of 5 minutes (retry_delay).

### How to Run
1. Ensure you have Apache Airflow installed and configured.
2. Copy the DAG file ordernow.py to your Airflow DAGs directory.
3. Start or restart your Airflow scheduler and webserver.
4. The DAG ordernow will be automatically detected and scheduled to run daily.


## Dependencies
- Python 3.x
- Boto3
- psycopg2
- redshift_connector
- pandas
- Apache Airflow
