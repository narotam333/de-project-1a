"""
### ETL DAG Tutorial Documentation
This ETL DAG is demonstrating an Extract -> Transform -> Load pipeline using Variables and runtime config
"""
# [START import_module]

# modules for documentation and datetime
from textwrap import dedent
import pendulum

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# module to handle variables
from airflow.models import Variable

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator

# Python module to generate weblog data
from project_modules.weblog_gen import generate_log

# Libraries/packages for AWS SDK
import os
import boto3
from botocore.exceptions import ClientError
import logging

# [END import_module]

# [START instantiate_dag]
with DAG(
    'weblog_s3_dag',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG tutorial',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
) as dag:
    # [END instantiate_dag]

    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    # [START weblog_function]
    def f_generate_log(*op_args, **kwargs):
        logging.info('Generating weblog file...')
        ti = kwargs['ti']
        lines = op_args[0]
        logFile = generate_log(lines)
        ti.xcom_push(key='logFileName', value=logFile)
        logging.info('weblog file generation completed...')
    # [END weblog_function]

    # [START s3_upload_file function]
    def s3_upload_file(**kwargs):
        ti = kwargs['ti']
        bucketName = kwargs['srcBucketName']    
        fileName = ti.xcom_pull(task_ids='weblog', key='logFileName')
        objectName = os.path.basename(fileName)

        s3_client = boto3.client('s3')
        try:
            logging.info('Uploading file '+fileName+' to AWS S3 bucket '+bucketName+' ...')
            logging.info('ObjectName is '+objectName)
            response = s3_client.upload_file(fileName, bucketName, objectName)
        except ClientError as e:
            return False
        logging.info('Upload completed...')
        Variable.set("fileName", fileName)
    # [END s3_upload_file function]
    

    ### Tasks ###

    create_weblog_task = PythonOperator(
        task_id='weblog',
        python_callable=f_generate_log,
        op_args = [30],
    )
    create_weblog_task.doc_md = dedent(
    """
    Weblog creation task using Python modules like Faker, numpy etc.
    """
    )

    s3_upload_log_file_task = PythonOperator(
        task_id = 's3_upload_log_file',
        python_callable=s3_upload_file,
        op_kwargs = {'srcBucketName': Variable.get("srcBucketName")},
    )
    s3_upload_log_file_task.doc_md = dedent(
    """
    Upload weblog file into AWS S3 bucket
    """
    )

    s3_transformation_task = S3FileTransformOperator(
        task_id='weblog_to_csv',
        source_s3_key='s3://'+Variable.get("srcBucketName")+'/'+Variable.get("fileName"),
        dest_s3_key='s3://'+'{{dag_run.conf["destBucketName"]}}'+'/weblog_'+str((pendulum.now("Europe/London")).strftime("%G%m%d-%H%M%S"))+'.csv',
        replace=False,
        transform_script='dags/project_modules/s3_transformation.py',
        #script_args=['<>'],
        source_aws_conn_id='s3conn',
        dest_aws_conn_id='s3conn',
    )
    s3_transformation_task.doc_md = dedent(
    """
    Transforming weblog file into csv using S3 operator
    """
    )

    create_weblog_task >> s3_upload_log_file_task >> s3_transformation_task

# [END main_flow]

# [END tutorial]
