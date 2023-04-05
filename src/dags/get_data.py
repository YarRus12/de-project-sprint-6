from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import logging

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"
log = logging.getLogger(__name__)


bucket_files = []


def fetch_s3_file(key: str):
    """Функция принимает в себя наименование файла и сохраняет его в папку с данными"""
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket='sprint6',
        Key=key,
        Filename=f'/data/{key}.csv'
    )
    bucket_files.append(key)
    log.info(f'Файл {key}csv загружен')


dag = DAG(
    schedule_interval=None,
    dag_id='get_data',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['Vertica'],
    is_paused_upon_creation=True)
groups_load = PythonOperator(task_id='group_data',
                                 python_callable=fetch_s3_file,
                                 op_kwargs={'bucket': 'data-bucket', 'key': 'groups'},
                                 dag=dag)
users_load = PythonOperator(task_id='users_data',
                                 python_callable=fetch_s3_file,
                                 op_kwargs={'bucket': 'data-bucket', 'key': 'users'},
                                 dag=dag)
dialogs_load = PythonOperator(task_id='dialogs_data',
                                 python_callable=fetch_s3_file,
                                 op_kwargs={'bucket': 'data-bucket', 'key': 'dialogs'},
                                 dag=dag)
group_log = PythonOperator(task_id='group_load',
                                 python_callable=fetch_s3_file,
                                 op_kwargs={'bucket': 'data-bucket', 'key': 'group_log.csv'},
                                 dag=dag)


groups_load, users_load, dialogs_load, group_log