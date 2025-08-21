import datetime
import pendulum
from airflow.sdk import DAG
from airflow import Dataset

from airflow.providers.standard.operators.bash import BashOperator
from airflow.decorators import task

dataset_dags_dataset_producer_1 = Dataset("dag_dataset_producer_1")
dataset_dags_dataset_producer_2 = Dataset("dag_dataset_producer_2")
with DAG(
    dag_id="dag_dataset_consumer_2",
    schedule= [dataset_dags_dataset_producer_1,dataset_dags_dataset_producer_2],
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
   
) as dag:

    bash_task = BashOperator(
        task_id = 'bash_task',
        outlets=[dataset_dags_dataset_producer_1],
        bash_command='echo {{ ti.run.id}} && echo "producer1 와 producer2 완료되면 실행"'
    )

   