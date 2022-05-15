# de-project-1a

Topic: How to use variables and runtime config in Apache Airflow

Steps executed:
- Write an ETL DAG and Task to generate weblog with dynamic filename 
- Write a Task to upload weblog into AWS S3 and store dynamic file name using variables
- Write a Task to process weblog file using S3FileTransformOperator, runtime config and variables 
- Execute the DAG using runtime config and check variables values
- Masking variable values in Airflow

Technology Stack used: Docker, Airflow, Python, AWS SDK , AWS CLI and AWS S3
