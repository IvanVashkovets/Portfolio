## Построение ETL пайплайна
#### статус: завершен

[Посмотреть HTML версию](https://ivanvashkovets.github.io/html_pages/Построение%20ETL%20пайплайна.html)

[Посмотреть Jupyter Notebook project](https://github.com/IvanVashkovets/Portfolio/blob/main/Построение%20ETL%20пайплайна/Построение%20ETL%20пайплайна.ipynb)

[Назад к списку проектов](https://github.com/IvanVashkovets/Portfolio/tree/main)

## Описание
Приложение, для которого необходимо выполнить задачу представляет собой ленту новостей и мессенджер. Для упрощения получения данных за предыдущий день, этот процесс необходимо автоматизировать при помощи Airflow.

## Задача
Написать DAG для Airflow, который ежедневно будет агрегировать данные за предыдущий день и помещать их в новую таблицу Clickhouse.

## Используемые библиотеки
airflow, clickhouse, sql, pandas, python
