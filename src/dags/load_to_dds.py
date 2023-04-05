from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import vertica_python


log = logging.getLogger(__name__)


conn_info = {'host': '51.250.75.20', 
             'port': '5433',
             'user': 'IAROSLAVRUSSUYANDEXRU',       
             'password': 'Fk0n81aSZe7tkRs',
             'database': 'dwh',
             'autocommit': True
}

def check_and_create(table_schema: str, suffix: str):
    """Функция принимает наименование схемы и суффикс типа таблицы
    Проверяет наличие таблиц в схеме и при их отсутствии создает таблицы"""
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        script_name = f'src/sql/ddl_{table_schema}_{suffix}.sql'
        cur.execute(open(script_name, 'r').read())
    log.info(f'Проверка {table_schema}{suffix} завершена успешно')

def load_to_dwh(table_schema: str, suffix: str):
    """Функция принимает наименование схемы и суффикс типа таблицы
        и загружает данные в таблицу"""
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        script_name = f'src/sql/loaddata_{table_schema}{suffix}.sql'
        cur.execute(open(script_name, 'r').read())
    log.info(f'Данные загружены в таблицы {table_schema}{suffix}')


dag = DAG(
    schedule_interval=None,
    dag_id='load_to_dds',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['DDS'],
    is_paused_upon_creation=True)
check_hubs = PythonOperator(task_id='check_hubs',
                                 python_callable=check_and_create,
                                 op_kwargs={'table_schema': 'IAROSLAVRUSSUYANDEXRU__DWH', 'suffix':'h'},
                                 dag=dag)
load_to_hubs = PythonOperator(task_id='load_to_hubs',
                                 python_callable=load_to_dwh,
                                 op_kwargs={'table_schema': 'IAROSLAVRUSSUYANDEXRU__DWH'},
                                 dag=dag)
check_links = PythonOperator(task_id='check_links',
                                 python_callable=check_and_create,
                                 op_kwargs={'table_schema': 'IAROSLAVRUSSUYANDEXRU__DWH', 'suffix':'l'},
                                 dag=dag)                                 
load_to_links= PythonOperator(task_id='load_to_links',
                                 python_callable=load_to_dwh,
                                 op_kwargs={'table_schema': 'IAROSLAVRUSSUYANDEXRU__DWH'},
                                 dag=dag)
check_sats = PythonOperator(task_id='check_sats',
                                 python_callable=check_and_create,
                                 op_kwargs={'table_schema': 'IAROSLAVRUSSUYANDEXRU__DWH', 'suffix':'s'},
                                 dag=dag)                                 
load_to_sats= PythonOperator(task_id='load_to_sats',
                                 python_callable=load_to_dwh,
                                 op_kwargs={'table_schema': 'IAROSLAVRUSSUYANDEXRU__DWH'},
                                 dag=dag)

check_hubs >> load_to_hubs >> check_links >> load_to_links >> check_sats >> load_to_sats