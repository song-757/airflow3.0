import pendulum
from airflow.sdk import DAG

from airflow.sensors.date_time import DateTimeSensorAsync

with DAG(
    dag_id="dags_time_sensor",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 8, 22, 0,0,0),
    end_date=pendulum.datetime(2025, 8, 22, 1,0,0),
    catchup=True,
) as dag:
    sync_sensor = DateTimeSensorAsync(
        task_id = "sync_sensor",
        target_time="""{{ macros.datetime.utcnow() +macros.timedelta(minute=5)}}"""
    )