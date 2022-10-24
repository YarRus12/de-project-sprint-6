from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from datetime import datetime
import logging
import vertica_python
from getpass import getpass


log = logging.getLogger(__name__)


conn_info = {'host': '51.250.75.20', 
             'port': '5433',
             'user': 'IAROSLAVRUSSUYANDEXRU',       
             'password': 'Fk0n81aSZe7tkRs',
             'database': 'dwh',
             # Вначале он нам понадобится, а дальше — решите позже сами
                         'autocommit': True
}

def check_and_create(table_schema, special=None):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        script_name = f'/lessons/dags/SQL_scripts/{table_schema}.sql'
        cur.execute(open(script_name, 'r').read())
    log.info(f'Проверка схемы {table_schema} завершена успешно')


def load_to_user(local_filename, schema, table_name, conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f"""COPY {schema}.{table_name} ( id, chat_name, registration_dt, country, age)
                        FROM LOCAL '/data/{local_filename}'
                        DELIMITER ','
                        REJECTED DATA AS TABLE {schema}.{table_name}_reg
                        ;""")

def load_to_groups(local_filename, schema, table_name, conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f"""COPY {schema}.{table_name} ( id, admin_id, group_name, registration_dt, is_private)
                        FROM LOCAL '/data/{local_filename}'
                        DELIMITER ','
                        REJECTED DATA AS TABLE {schema}.{table_name}_reg
                        ;""")

def load_to_dialogs(local_filename, schema, table_name, conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f"""COPY {schema}.{table_name} ( message_id,message_ts,message_from,message_to,message,message_group)
                        FROM LOCAL '/data/{local_filename}'
                        DELIMITER ','
                        REJECTED DATA AS TABLE {schema}.{table_name}_reg
                        ;""")
def group_log(local_filename, schema, table_name, conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f"""COPY {schema}.{table_name} (group_id,user_id,user_id_from,event,datetime_ts)
                        FROM LOCAL '/data/{local_filename}'
                        DELIMITER ','
                        REJECTED DATA AS TABLE {schema}.{table_name}_reg
                        ;""")


dag = DAG(
    schedule_interval=None,
    dag_id='load_to_stage',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['STAGING'],
    is_paused_upon_creation=True)
check = PythonOperator(task_id='check',
                                 python_callable=check_and_create,
                                 op_kwargs={'table_schema': 'IAROSLAVRUSSUYANDEXRU__STAGING'},
                                 dag=dag)
load_users = PythonOperator(task_id='load_to_user',
                                 python_callable=load_to_user,
                                 op_kwargs={'local_filename': 'users.csv', 'schema': 'IAROSLAVRUSSUYANDEXRU__STAGING', 'table_name':'users'},
                                 dag=dag)
load_groups = PythonOperator(task_id='load_to_groups',
                                 python_callable=load_to_groups,
                                 op_kwargs={'local_filename': 'groups.csv', 'schema': 'IAROSLAVRUSSUYANDEXRU__STAGING', 'table_name':'groups'},
                                 dag=dag)
load_dialogs = PythonOperator(task_id='load_to_dialogs',
                                 python_callable=load_to_dialogs,
                                 op_kwargs={'local_filename': 'dialogs.csv', 'schema': 'IAROSLAVRUSSUYANDEXRU__STAGING', 'table_name':'dialogs'},
                                 dag=dag)
load_group_logs = PythonOperator(task_id='load_group_logs',
                                 python_callable=group_log,
                                 op_kwargs={'local_filename': 'group_log.csv', 'schema': 'IAROSLAVRUSSUYANDEXRU__STAGING', 'table_name':'group_log'},
                                 dag=dag)

check >> load_users >> load_groups >> load_dialogs >> load_group_logs