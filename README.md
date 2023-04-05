# Проект 6-го спринта

### Описание
Репозиторий предназначен для сдачи проекта 6-го спринта.

Проект предусматривает построение ETL-процесса для витрины с группами в социальной сети, в которых начала общаться большая часть их участников - паблики с высокой конверсией в первое сообщение.

Источник данных - S3.
Данные сохраняются в stage, далее инкрементируются в dds-слой модели DataVault


### Структура репозитория
Внутри `src` расположены две папки:
- `/src/dags`;
- `/src/sql`.

### Инструентарий
Python, Vertica, Airflow, DataVault