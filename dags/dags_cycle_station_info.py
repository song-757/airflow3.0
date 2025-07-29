import datetime
import pendulum
from airflow import DAG

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator

with DAG(
    dag_id="dags_cycle_station_info",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    task_cyckle_station_info = HttpOperator(
        task_id = 'task_cyckle_station_info',
        http_conn_id = 'openapi.seou.go.kr',
        endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleStationInfo/1/10',
        method = 'GET',
        heaaders={
            'Content-Type','application/json',
            'charset':'utf-8',
            'Accept':'*/*'
        }
    )
    task_cyckle_station_info