
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


from utils.help import retrieve_api_data_and_upload_to_s3, transform_and_upload_to_s3, copy_from_s3_to_redshift


with DAG(
    "Jobs_data",
    default_args={
        "depends_on_past": False,
        "email": ["dataengineering@10alytics.org"],
        "email_on_failure": True,
        "email_on_retry": True,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="10Alytics Job Boards Data",
    schedule='@daily',
    start_date=datetime(2024, 1, 7),
) as dag:
      
    begin_execution = DummyOperator(task_id='begin_execution')

    retrieve_api_data_and_upload_to_s3 = PythonOperator(task_id = 'extract_data', python_callable = retrieve_api_data_and_upload_to_s3)


    transform_and_upload_to_s3 = PythonOperator(task_id = 'transform_data', python_callable = transform_and_upload_to_s3)


    copy_from_s3_to_redshift = PythonOperator(task_id = 'copy_to_redshift', python_callable = copy_from_s3_to_redshift)


    end_execution = DummyOperator(task_id='end_execution')



#Start function calls
    begin_execution >> retrieve_api_data_and_upload_to_s3

    retrieve_api_data_and_upload_to_s3 >> transform_and_upload_to_s3

    transform_and_upload_to_s3 >> copy_from_s3_to_redshift

    copy_from_s3_to_redshift >> end_execution