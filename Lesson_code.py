import vertica_python
from getpass import getpass
import codecs

conn_info = {'host': '51.250.75.20',
      'port': '5433',
      'user': 'IAROSLAVRUSSUYANDEXRU',    
      'password': 'Fk0n81aSZe7tkRs',
      'database': 'dwh',
      # Вначале он нам понадобится, а дальше — решите позже сами
            'autocommit': True
}
def try_select(conn_info=conn_info):
  # И рекомендуем использовать соединение вот так
  with vertica_python.connect(**conn_info) as conn:
    cur = conn.cursor()
    cur.execute('Select 1 as a1') 
    res = cur.fetchall()
    return res
try_select(conn_info=conn_info)


vertica_user = 'IAROSLAVRUSSUYANDEXRU'

import csv
from pathlib import Path
dataset = 'test_dataset.csv'
N = 10000 # на этот раз можете поставить даже 10 млн
with open(dataset, 'w') as csvfile:
    fwriter = csv.writer(csvfile, delimiter='|')
    for i in range(N):
        fwriter.writerow([i, 'asds'])
# эта команда напечатает абсолютный путь к файлу, скопируйте его
print(Path(dataset).resolve())
# а это пара первых строк для визуализации результата:
with open(dataset, 'r') as csvfile:
    for i in range(5):
        print(csvfile.readline(), end='')




import boto3

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

session = boto3.session.Session()
s3_client = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)
s3_client.download_file(
    Bucket='sprint6',
    Key='groups.csv',
    Filename='/data/groups.csv'
)

s3_client = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

s3_client.download_file(
      Bucket='sprint6',
      Key='groups.csv',
      Filename='/data/groups.csv'
)